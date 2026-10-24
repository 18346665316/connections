import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:flutter/foundation.dart';
import '../models/peer_device.dart';
import '../utils/transfer_logger.dart';
import 'settings_service.dart';

class DiscoveryService extends ChangeNotifier {
  static const int discoveryUdpPort = 53317;
  static const String multicastGroup = '224.0.0.167';
  static const int defaultTransferPort = 53318;
  static const Duration announceInterval = Duration(seconds: 3);
  static const Duration peerTimeout = Duration(seconds: 12);

  late PeerDevice _localDevice;
  RawDatagramSocket? _udpSocket;
  Timer? _announceTimer;
  Timer? _cleanupTimer;
  bool _isScanningSubnet = false;

  final Map<String, PeerDevice> _peers = {};
  final Set<String> _inFlightPeerPolls = {};

  /// 当在心跳响应中收到对端挂起的反向拉取传输请求时触发（用于穿透 Android 模拟器 10.0.2.16 等单向 NAT）
  void Function(PeerDevice peer, List<Map<String, dynamic>> pullOffers)?
      onPullOffersReceived;

  PeerDevice get localDevice => _localDevice;
  bool get isScanningSubnet => _isScanningSubnet;

  List<PeerDevice> get peers {
    final list = _peers.values.toList();
    list.sort((a, b) => a.alias.compareTo(b.alias));
    return list;
  }

  DiscoveryService() {
    _localDevice = _createInitialLocalDevice();
  }

  PeerDevice _createInitialLocalDevice() {
    final persisted = SettingsService.load();
    final rng = Random();
    final osName = PeerDevice.detectLocalOsName();
    final defaultAlias = _resolveDefaultAlias(osName);

    const legacyPrefixes = [
      '星辰 ',
      '极速 ',
      '幻影 ',
      '量子 ',
      '深海 ',
      '云端 ',
      '流光 ',
      '赤焰 ',
    ];
    final rawPersistedAlias = persisted.alias?.trim() ?? '';
    final isLegacyAiAlias =
        legacyPrefixes.any((prefix) => rawPersistedAlias.startsWith(prefix));

    final alias = (rawPersistedAlias.isNotEmpty && !isLegacyAiAlias)
        ? rawPersistedAlias
        : defaultAlias;
    final avatar = osName.toLowerCase();
    final id = persisted.deviceId ??
        '${DateTime.now().millisecondsSinceEpoch}-${rng.nextInt(999999).toString().padLeft(6, '0')}';

    if (persisted.deviceId == null || isLegacyAiAlias) {
      unawaited(
        SettingsService.save(
          persisted.copyWith(
            deviceId: id,
            alias: alias,
            avatar: avatar,
          ),
        ),
      );
    }

    return PeerDevice(
      id: id,
      alias: alias,
      avatar: avatar,
      deviceType: PeerDevice.detectLocalDeviceType(),
      os: osName,
      ip: '127.0.0.1',
      transferPort: defaultTransferPort,
      lastSeen: DateTime.now(),
    );
  }

  String _resolveDefaultAlias(String osName) {
    try {
      var host = Platform.localHostname.trim();
      if (host.endsWith('.local')) {
        host = host.substring(0, host.length - 6);
      } else if (host.endsWith('.lan')) {
        host = host.substring(0, host.length - 4);
      }
      if (host.isNotEmpty &&
          host.toLowerCase() != 'localhost' &&
          !host.startsWith('android-')) {
        return host;
      }
    } catch (_) {}
    return osName;
  }

  Future<void> initialize({required int boundTransferPort}) async {
    final detectedIp = await detectLocalIpAddress();
    final effectiveId = boundTransferPort == defaultTransferPort
        ? _localDevice.id
        : '${_localDevice.id}-$boundTransferPort';
    _localDevice = _localDevice.copyWith(
      id: effectiveId,
      ip: detectedIp,
      transferPort: boundTransferPort,
      lastSeen: DateTime.now(),
    );
    TransferLogger.discovery(
      '本机节点上线: ${_localDevice.alias} (${_localDevice.os}) '
      '| 地址: ${_localDevice.ip}:${_localDevice.transferPort} '
      '| 短码: ${_localDevice.shortCode}',
    );
    notifyListeners();

    await _startUdpDiscovery();

    _announceTimer?.cancel();
    _announceTimer = Timer.periodic(announceInterval, (_) async {
      final ip = await detectLocalIpAddress();
      if (ip != _localDevice.ip) {
        _localDevice = _localDevice.copyWith(ip: ip);
        notifyListeners();
      }
      _sendUdpAnnouncement(type: 'announce');
    });

    _cleanupTimer?.cancel();
    _cleanupTimer = Timer.periodic(const Duration(seconds: 2), (_) {
      _removeStalePeers();
      _pollKnownPeersHttp();
    });

    // 启动后立即广播并探测本机多开实例端口
    _sendUdpAnnouncement(type: 'announce');
    unawaited(_probeLocalhostPorts());
  }

  void updateProfile({String? alias, String? avatar}) {
    _localDevice = _localDevice.copyWith(
      alias: alias?.trim().isNotEmpty == true ? alias!.trim() : _localDevice.alias,
      avatar: avatar ?? _localDevice.avatar,
      lastSeen: DateTime.now(),
    );
    final current = SettingsService.load();
    unawaited(
      SettingsService.save(
        current.copyWith(
          deviceId: _localDevice.id,
          alias: _localDevice.alias,
          avatar: _localDevice.avatar,
        ),
      ),
    );
    notifyListeners();
    _sendUdpAnnouncement(type: 'announce');
  }

  Future<String> detectLocalIpAddress() async {
    try {
      final interfaces = await NetworkInterface.list(
        includeLoopback: false,
        type: InternetAddressType.IPv4,
      );

      final candidates = <({String ip, int score})>[];

      for (final iface in interfaces) {
        final name = iface.name.toLowerCase();
        // 排除常见虚拟网卡、VPN 隧道与容器网桥
        final isVirtual = name.startsWith('utun') ||
            name.startsWith('tun') ||
            name.startsWith('tap') ||
            name.startsWith('ppp') ||
            name.startsWith('bridge') ||
            name.startsWith('docker') ||
            name.startsWith('veth') ||
            name.startsWith('vmnet') ||
            name.startsWith('vboxnet') ||
            name.contains('tailscale') ||
            name.contains('ZeroTier');

        final isPreferredNic = name == 'en0' ||
            name == 'en1' ||
            name.startsWith('wlan') ||
            name.startsWith('eth') ||
            name.contains('wi-fi');

        for (final addr in iface.addresses) {
          final ip = addr.address;
          if (ip.startsWith('169.254.') || ip.startsWith('198.18.')) {
            // 忽略链路本地地址与常见代理 Fake-IP 网段
            continue;
          }

          int score = 0;
          if (!isVirtual) score += 100;
          if (isPreferredNic) score += 50;
          if (ip.startsWith('192.168.')) {
            score += 30;
          } else if (_isPrivate172(ip)) {
            score += 20;
          } else if (ip.startsWith('10.')) {
            score += 15;
          }

          candidates.add((ip: ip, score: score));
        }
      }

      if (candidates.isNotEmpty) {
        candidates.sort((a, b) => b.score.compareTo(a.score));
        return candidates.first.ip;
      }
    } catch (_) {}
    return '127.0.0.1';
  }

  bool _isPrivate172(String ip) {
    final parts = ip.split('.');
    if (parts.length != 4 || parts[0] != '172') return false;
    final second = int.tryParse(parts[1]) ?? 0;
    return second >= 16 && second <= 31;
  }

  Future<void> _startUdpDiscovery() async {
    try {
      try {
        _udpSocket = await RawDatagramSocket.bind(
          InternetAddress.anyIPv4,
          discoveryUdpPort,
          reuseAddress: true,
          reusePort: !Platform.isWindows,
        );
      } catch (_) {
        _udpSocket = await RawDatagramSocket.bind(
          InternetAddress.anyIPv4,
          discoveryUdpPort,
          reuseAddress: true,
        );
      }

      _udpSocket!.broadcastEnabled = true;
      _udpSocket!.multicastHops = 4;

      try {
        final multicastAddr = InternetAddress(multicastGroup);
        _udpSocket!.joinMulticast(multicastAddr);
        final interfaces = await NetworkInterface.list(
          includeLoopback: false,
          type: InternetAddressType.IPv4,
        );
        for (final iface in interfaces) {
          try {
            _udpSocket!.joinMulticast(multicastAddr, iface);
          } catch (_) {}
        }
      } catch (_) {}

      _udpSocket!.listen(
        (RawSocketEvent event) {
          if (event == RawSocketEvent.read) {
            final dg = _udpSocket?.receive();
            if (dg != null) {
              _handleIncomingDatagram(dg);
            }
          } else if (event == RawSocketEvent.closed) {
            _udpSocket = null;
          }
        },
        onError: (Object _) {
          // 忽略部分网卡在离线或路由受限时的底层 UDP 异步错误
        },
        onDone: () {
          _udpSocket = null;
        },
      );
    } catch (e) {
      debugPrint('UDP 监听启动失败 (可能端口被独占，将使用 HTTP 发现兜底): $e');
    }
  }

  void _handleIncomingDatagram(Datagram dg) {
    try {
      final text = utf8.decode(dg.data, allowMalformed: true);
      final map = jsonDecode(text) as Map<String, dynamic>;
      final type = map['type'] as String? ?? 'announce';
      final deviceMap = map['device'] as Map<String, dynamic>?;
      if (deviceMap == null) return;

      final senderIp = dg.address.address;
      final peer = PeerDevice.fromJson(deviceMap, fallbackIp: senderIp);

      if (peer.id == _localDevice.id) return;

      if (type == 'goodbye') {
        final removed = _peers.remove(peer.id);
        if (removed != null) {
          TransferLogger.discovery(
            '设备下线: ${removed.alias} (${removed.ip}:${removed.transferPort})',
          );
          notifyListeners();
        }
        return;
      }

      // 当数据包来自其他物理设备的真实局域网 IP 时，优先使用实际可达的来源 IP；若来自本机回环或本机 NAT（如本机 Android 模拟器），则保留对方申报的 IP
      final isLoopbackOrSelf = senderIp == '127.0.0.1' ||
          senderIp == '::1' ||
          senderIp == '0.0.0.0' ||
          senderIp == _localDevice.ip;
      final effectiveIp = isLoopbackOrSelf ? peer.ip : senderIp;
      final updatedPeer = peer.copyWith(
        ip: effectiveIp,
        lastSeen: DateTime.now(),
      );

      final isNew = !_peers.containsKey(updatedPeer.id);
      _peers[updatedPeer.id] = updatedPeer;
      if (isNew) {
        TransferLogger.discovery(
          'UDP 发现新设备: ${updatedPeer.alias} (${updatedPeer.os}) '
          '@ ${updatedPeer.ip}:${updatedPeer.transferPort} [${updatedPeer.shortCode}]',
        );
      }
      notifyListeners();

      if (type == 'announce' || isNew) {
        // 立即单播回应 UDP + HTTP 注册，确保双向瞬间可见
        _sendUdpReply(dg.address, dg.port);
        unawaited(_registerWithPeerHttp(updatedPeer));
      }
    } catch (_) {}
  }

  void _sendUdpAnnouncement({required String type}) {
    final socket = _udpSocket;
    if (socket == null) return;

    final payload = utf8.encode(jsonEncode({
      'type': type,
      'device': _localDevice.toJson(),
    }));

    final targets = <String>{
      multicastGroup,
      '255.255.255.255',
      '127.0.0.1',
    };

    final parts = _localDevice.ip.split('.');
    if (parts.length == 4 && parts[0] != '127') {
      targets.add('${parts[0]}.${parts[1]}.${parts[2]}.255');
    }

    for (final addr in targets) {
      try {
        socket.send(payload, InternetAddress(addr), discoveryUdpPort);
      } catch (_) {}
    }
  }

  void _sendUdpReply(InternetAddress targetAddress, int targetPort) {
    final socket = _udpSocket;
    if (socket == null) return;
    try {
      final payload = utf8.encode(jsonEncode({
        'type': 'reply',
        'device': _localDevice.toJson(),
      }));
      socket.send(payload, targetAddress, targetPort);
      if (targetPort != discoveryUdpPort) {
        socket.send(payload, targetAddress, discoveryUdpPort);
      }
    } catch (_) {}
  }

  /// 当收到 HTTP 握手或主动扫描发现节点时注册到在线列表
  void registerPeer(PeerDevice peer) {
    if (peer.id == _localDevice.id) return;
    if (peer.transferPort <= 0 || peer.deviceType == DeviceType.web) return;
    final isNew = !_peers.containsKey(peer.id);
    _peers[peer.id] = peer.copyWith(lastSeen: DateTime.now());
    if (isNew) {
      TransferLogger.discovery(
        'HTTP 注册新设备: ${peer.alias} (${peer.os}) '
        '@ ${peer.ip}:${peer.transferPort} [${peer.shortCode}]',
      );
    }
    notifyListeners();
  }

  /// 主动从在线列表中移除指定设备
  void removePeer(String peerId) {
    if (_peers.remove(peerId) != null) {
      notifyListeners();
    }
  }

  void _pollKnownPeersHttp() {
    final localIsEmulator = _localDevice.ip.startsWith('10.0.2.');
    if (localIsEmulator && !_peers.values.any((p) => p.ip == '10.0.2.2')) {
      unawaited(_probeLocalhostPorts());
    }
    for (final peer in _peers.values.toList()) {
      // 宿主机不主动轮询模拟器内网 IP 10.0.2.16（由模拟器单向轮询宿主机）
      if (!localIsEmulator && peer.ip.startsWith('10.0.2.')) {
        continue;
      }
      unawaited(_registerWithPeerHttp(peer));
    }
  }

  Future<void> _registerWithPeerHttp(PeerDevice peer) async {
    if (!_inFlightPeerPolls.add(peer.id)) return;
    final client = HttpClient()
      ..connectionTimeout = const Duration(milliseconds: 1200);
    try {
      final uri =
          Uri.parse('http://${peer.ip}:${peer.transferPort}/api/register');
      final req = await client.postUrl(uri);
      req.headers.contentType = ContentType.json;
      req.add(utf8.encode(jsonEncode(_localDevice.toJson())));
      final resp = await req.close();
      if (resp.statusCode == HttpStatus.ok) {
        final body = await utf8.decoder.bind(resp).join();
        final map = jsonDecode(body) as Map<String, dynamic>;
        // 刷新该节点的在线心跳时间并同步最新名称
        final existing = _peers[peer.id];
        if (existing != null) {
          final remoteAlias = (map['alias'] as String?)?.trim();
          final nextAlias = (remoteAlias != null && remoteAlias.isNotEmpty)
              ? remoteAlias
              : existing.alias;
          final aliasChanged = nextAlias != existing.alias;
          _peers[peer.id] = existing.copyWith(
            alias: nextAlias,
            lastSeen: DateTime.now(),
          );
          if (aliasChanged) {
            notifyListeners();
          }
        }
        final rawOffers = map['pullOffers'] as List<dynamic>?;
        if (rawOffers != null &&
            rawOffers.isNotEmpty &&
            onPullOffersReceived != null) {
          final offers = rawOffers
              .whereType<Map<String, dynamic>>()
              .toList();
          if (offers.isNotEmpty) {
            onPullOffersReceived!(peer, offers);
          }
        }
      } else {
        await resp.drain<void>();
      }
    } catch (_) {
    } finally {
      _inFlightPeerPolls.remove(peer.id);
      client.close();
    }
  }

  /// 探测本机多开端口（53318~53325）以及 Android 模拟器宿主机网关（10.0.2.2）
  Future<void> _probeLocalhostPorts() async {
    final isAndroidEmulator = _localDevice.ip.startsWith('10.0.2.');
    for (int port = defaultTransferPort; port <= defaultTransferPort + 7; port++) {
      if (port == _localDevice.transferPort && !isAndroidEmulator) continue;
      unawaited(_probeSingleEndpoint('127.0.0.1', port));
      if (isAndroidEmulator) {
        // Android 模拟器内通过 10.0.2.2 直接访问宿主机 Mac/PC
        unawaited(_probeSingleEndpoint('10.0.2.2', port));
      } else if (_localDevice.ip != '127.0.0.1') {
        unawaited(_probeSingleEndpoint(_localDevice.ip, port));
      }
    }
  }

  /// 通过短码（如 105 / #105）或完整 IP(:端口) 手动连接设备
  Future<PeerDevice?> connectByCodeOrIp(String rawInput) async {
    var input = rawInput.trim();
    if (input.isEmpty) return null;
    if (input.startsWith('#')) {
      input = input.substring(1).trim();
    }

    String targetIp = input;
    int? explicitPort;

    if (input.contains(':')) {
      final split = input.split(':');
      targetIp = split[0].trim();
      explicitPort = int.tryParse(split[1].trim());
    }

    // 如果只输入了 1~3 位数字短码（例如 "105"），自动拼接当前局域网网段前缀
    if (RegExp(r'^\d{1,3}$').hasMatch(targetIp)) {
      final parts = _localDevice.ip.split('.');
      if (parts.length == 4 && parts[0] != '127') {
        targetIp = '${parts[0]}.${parts[1]}.${parts[2]}.$targetIp';
      } else {
        targetIp = '127.0.0.1';
      }
    }

    if (explicitPort != null) {
      return _probeSingleEndpoint(targetIp, explicitPort);
    }

    // 默认探测标准端口及相邻备用端口
    for (int port = defaultTransferPort; port <= defaultTransferPort + 5; port++) {
      final found = await _probeSingleEndpoint(targetIp, port);
      if (found != null) return found;
    }
    return null;
  }

  /// 快速扫描当前 /24 子网内所有在线节点（应对开启了 UDP 广播隔离的路由器）
  Future<int> scanSubnet() async {
    if (_isScanningSubnet) return 0;
    _isScanningSubnet = true;
    notifyListeners();

    int discoveredCount = 0;
    try {
      _sendUdpAnnouncement(type: 'announce');
      await _probeLocalhostPorts();

      final parts = _localDevice.ip.split('.');
      if (parts.length == 4 && parts[0] != '127') {
        final prefix = '${parts[0]}.${parts[1]}.${parts[2]}';
        final batchSize = 32;
        for (int start = 1; start <= 254; start += batchSize) {
          final futures = <Future<PeerDevice?>>[];
          for (int i = start; i < start + batchSize && i <= 254; i++) {
            final candidateIp = '$prefix.$i';
            if (candidateIp == _localDevice.ip) continue;
            futures.add(
              _probeSingleEndpoint(
                candidateIp,
                defaultTransferPort,
                timeout: const Duration(milliseconds: 350),
              ),
            );
          }
          final results = await Future.wait(futures);
          discoveredCount += results.whereType<PeerDevice>().length;
        }
      }
    } finally {
      _isScanningSubnet = false;
      notifyListeners();
    }
    return discoveredCount;
  }

  Future<PeerDevice?> _probeSingleEndpoint(
    String ip,
    int port, {
    Duration timeout = const Duration(milliseconds: 600),
  }) async {
    if (ip == _localDevice.ip && port == _localDevice.transferPort) {
      return null;
    }
    if (ip == '127.0.0.1' && port == _localDevice.transferPort) {
      return null;
    }

    final client = HttpClient()..connectionTimeout = timeout;
    try {
      final uri = Uri.parse('http://$ip:$port/api/info');
      final req = await client.getUrl(uri).timeout(timeout);
      req.headers.set('X-Peer-Info', base64Encode(utf8.encode(jsonEncode(_localDevice.toJson()))));
      final resp = await req.close().timeout(timeout);
      if (resp.statusCode == 200) {
        final body = await utf8.decoder.bind(resp).join().timeout(timeout);
        final json = jsonDecode(body) as Map<String, dynamic>;
        final parsed = PeerDevice.fromJson(json, fallbackIp: ip);
        final effectiveIp =
            (ip == '127.0.0.1' && parsed.ip != '127.0.0.1') ? parsed.ip : ip;
        final peer = parsed.copyWith(
          ip: effectiveIp,
          transferPort: port,
          lastSeen: DateTime.now(),
        );
        if (peer.id != _localDevice.id) {
          registerPeer(peer);
          unawaited(_registerWithPeerHttp(peer));
          return peer;
        }
      }
    } catch (_) {
    } finally {
      client.close(force: true);
    }
    return null;
  }

  void _removeStalePeers() {
    final now = DateTime.now();
    bool removed = false;
    _peers.removeWhere((_, peer) {
      final expired = now.difference(peer.lastSeen) > peerTimeout;
      if (expired) removed = true;
      return expired;
    });
    if (removed) {
      notifyListeners();
    }
  }

  Future<void> handleAppLifecycleResume() async {
    final ip = await detectLocalIpAddress();
    if (ip != _localDevice.ip) {
      _localDevice = _localDevice.copyWith(ip: ip, lastSeen: DateTime.now());
      notifyListeners();
    }
    if (_udpSocket == null) {
      await _startUdpDiscovery();
    }
    refreshDiscovery();
  }

  void refreshDiscovery() {
    _sendUdpAnnouncement(type: 'announce');
    unawaited(_probeLocalhostPorts());
  }

  @override
  void dispose() {
    _announceTimer?.cancel();
    _cleanupTimer?.cancel();
    _sendUdpAnnouncement(type: 'goodbye');
    _udpSocket?.close();
    super.dispose();
  }
}
