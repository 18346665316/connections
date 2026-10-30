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
  static const Duration busyPeerTimeout = Duration(seconds: 45);

  late PeerDevice _localDevice;
  RawDatagramSocket? _udpSocket;
  Timer? _announceTimer;
  Timer? _cleanupTimer;
  bool _isScanningSubnet = false;
  int _cleanupTickCount = 0;

  final HttpClient _httpClient = HttpClient()
    ..findProxy = null
    ..connectionTimeout = const Duration(milliseconds: 1200)
    ..idleTimeout = const Duration(seconds: 25)
    ..maxConnectionsPerHost = 2;

  final Map<String, PeerDevice> _peers = {};
  final Set<String> _inFlightPeerPolls = {};
  final Map<String, DateTime> _lastUdpReplyTime = {};

  /// 当存在高速文件传输时返回 true，自动静默周期性 UDP 广播并降频 HTTP 轮询，消除无线空口自干扰
  bool Function()? isTransferBusy;

  /// 当在心跳响应中收到对端挂起的反向拉取传输请求时触发（用于穿透 Android 模拟器 10.0.2.16 等单向 NAT）
  void Function(PeerDevice peer, List<Map<String, dynamic>> pullOffers)?
      onPullOffersReceived;

  PeerDevice get localDevice => _localDevice;
  List<String> get localAddresses => _localDevice.candidateIps;
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
    final ipInfo = await detectLocalIpAddresses();
    final effectiveId = boundTransferPort == defaultTransferPort
        ? _localDevice.id
        : '${_localDevice.id}-$boundTransferPort';
    _localDevice = _localDevice.copyWith(
      id: effectiveId,
      ip: ipInfo.primaryIp,
      addresses: ipInfo.allIps,
      transferPort: boundTransferPort,
      lastSeen: DateTime.now(),
    );
    TransferLogger.discovery(
      '本机节点上线: ${_localDevice.alias} (${_localDevice.os}) '
      '| 地址: ${_localDevice.ip}:${_localDevice.transferPort} '
      '${ipInfo.allIps.length > 1 ? "(多网卡: ${ipInfo.allIps.join(", ")}) " : ""}'
      '| 短码: ${_localDevice.shortCode}',
    );
    notifyListeners();

    await _startUdpDiscovery();

    _announceTimer?.cancel();
    _announceTimer = Timer.periodic(announceInterval, (_) async {
      // 传输期间自动静默周期性 UDP 广播与网卡枚举，避免组播/广播拉低 Wi-Fi 空口调制速率
      if (isTransferBusy?.call() == true) {
        return;
      }
      final updated = await detectLocalIpAddresses();
      if (updated.primaryIp != _localDevice.ip ||
          !listEquals(updated.allIps, _localDevice.addresses)) {
        _localDevice = _localDevice.copyWith(
          ip: updated.primaryIp,
          addresses: updated.allIps,
        );
        notifyListeners();
      }
      _sendUdpAnnouncement(type: 'announce');
    });

    _cleanupTimer?.cancel();
    _cleanupTimer = Timer.periodic(const Duration(seconds: 2), (_) {
      _cleanupTickCount++;
      final busy = isTransferBusy?.call() == true;
      _removeStalePeers(busy: busy);
      // 传输进行中将后台 HTTP 心跳轮询降频至 12 秒一次，将无线空口与连接池 100% 让给数据流
      if (!busy || _cleanupTickCount % 6 == 0) {
        _pollKnownPeersHttp();
      }
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
    final res = await detectLocalIpAddresses();
    return res.primaryIp;
  }

  /// 检测本机所有可用网卡 IP（原生支持 macOS bridge0 雷雳网桥、有线以太网与多网段优先级排序）
  Future<({String primaryIp, List<String> allIps})>
      detectLocalIpAddresses() async {
    try {
      final interfaces = await NetworkInterface.list(
        includeLoopback: false,
        type: InternetAddressType.IPv4,
      );

      final candidates = <({String ip, int score})>[];

      for (final iface in interfaces) {
        final name = iface.name.toLowerCase();
        // 识别 macOS 雷雳网桥 (Thunderbolt Bridge: bridge0 / bridge1)
        final isThunderboltBridge =
            Platform.isMacOS && (name == 'bridge0' || name == 'bridge1');

        // 排除 VPN 隧道与容器虚拟网桥（保留 macOS 雷雳网桥）
        final isVirtual = !isThunderboltBridge &&
            (name.startsWith('utun') ||
                name.startsWith('tun') ||
                name.startsWith('tap') ||
                name.startsWith('ppp') ||
                name.startsWith('bridge') ||
                name.startsWith('br-') ||
                name.startsWith('docker') ||
                name.startsWith('veth') ||
                name.startsWith('vmnet') ||
                name.startsWith('vboxnet') ||
                name.contains('tailscale') ||
                name.contains('zerotier'));

        final isPreferredNic = isThunderboltBridge ||
            name == 'en0' ||
            name == 'en1' ||
            name == 'en2' ||
            name == 'en3' ||
            name.startsWith('wlan') ||
            name.startsWith('eth') ||
            name.contains('wi-fi');

        for (final addr in iface.addresses) {
          final ip = addr.address;
          // 忽略代理软件 Fake-IP 网段
          if (ip.startsWith('198.18.')) {
            continue;
          }
          final isLinkLocal = ip.startsWith('169.254.');
          // 仅允许物理网卡或雷雳网桥上的 169.254.x.x 直连链路本地地址作为候选
          if (isLinkLocal && isVirtual) {
            continue;
          }

          int score = 0;
          if (!isVirtual) score += 100;
          if (isThunderboltBridge && !isLinkLocal) {
            // 若雷雳网桥配置了私有网段 IP（如 10.x / 192.168.x），赋予最高网卡优先级
            score += 65;
          } else if (isPreferredNic) {
            score += 50;
          }

          if (ip.startsWith('192.168.')) {
            score += 30;
          } else if (_isPrivate172(ip)) {
            score += 20;
          } else if (ip.startsWith('10.')) {
            score += 15;
          } else if (isLinkLocal) {
            // 169.254.x.x 雷雳/网线直连无 DHCP 时的自协商地址：放入多网卡候选列表，无局域网时兜底为主地址
            score -= 60;
          }

          candidates.add((ip: ip, score: score));
        }
      }

      if (candidates.isNotEmpty) {
        candidates.sort((a, b) => b.score.compareTo(a.score));
        final allIps = <String>[];
        for (final c in candidates) {
          if (!allIps.contains(c.ip)) {
            allIps.add(c.ip);
          }
        }
        return (primaryIp: allIps.first, allIps: allIps);
      }
    } catch (_) {}
    return (primaryIp: '127.0.0.1', allIps: const <String>['127.0.0.1']);
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
          _localDevice.candidateIps.contains(senderIp);
      final effectiveIp = isLoopbackOrSelf ? peer.ip : senderIp;

      final existing = _peers[peer.id];
      final isNew = existing == null;
      final mergedAddresses = <String>{
        ...peer.addresses,
        if (existing != null) ...existing.addresses,
        if (existing != null &&
            existing.ip.isNotEmpty &&
            existing.ip != '0.0.0.0' &&
            existing.ip != effectiveIp)
          existing.ip,
      }.toList();
      final updatedPeer = peer.copyWith(
        ip: effectiveIp,
        addresses: mergedAddresses,
        lastSeen: DateTime.now(),
      );
      final metadataChanged = isNew ||
          existing.alias != updatedPeer.alias ||
          existing.ip != updatedPeer.ip ||
          existing.transferPort != updatedPeer.transferPort ||
          existing.os != updatedPeer.os ||
          existing.avatar != updatedPeer.avatar ||
          !listEquals(existing.addresses, mergedAddresses);
      _peers[updatedPeer.id] = updatedPeer;
      if (isNew) {
        TransferLogger.discovery(
          'UDP 发现新设备: ${updatedPeer.alias} (${updatedPeer.os}) '
          '@ ${updatedPeer.ip}:${updatedPeer.transferPort} [${updatedPeer.shortCode}]',
        );
      }
      if (metadataChanged) {
        notifyListeners();
      }

      if (type == 'announce' || isNew) {
        final now = DateTime.now();
        final lastReply = _lastUdpReplyTime[updatedPeer.id];
        // 对同一设备短时间内的多路组播/广播副本进行去重节流，避免高频风暴抢占无线信道
        if (isNew ||
            lastReply == null ||
            now.difference(lastReply) > const Duration(milliseconds: 1500)) {
          _lastUdpReplyTime[updatedPeer.id] = now;
          _sendUdpReply(dg.address, dg.port);
          unawaited(_registerWithPeerHttp(updatedPeer));
        }
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

    for (final ip in _localDevice.candidateIps) {
      final parts = ip.split('.');
      if (parts.length == 4 && parts[0] != '127') {
        targets.add('${parts[0]}.${parts[1]}.${parts[2]}.255');
        // 原生兼容 /23 企业大子网（如 192.168.20.x 与 192.168.21.x 共享 192.168.21.255 广播域）
        final thirdOctet = int.tryParse(parts[2]);
        if (thirdOctet != null && thirdOctet >= 0 && thirdOctet <= 255) {
          targets.add('${parts[0]}.${parts[1]}.${thirdOctet ^ 1}.255');
        }
      }
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

  /// 传输进行中由数据流实时刷新对端活跃时间，避免静默期误判离线且不触发多余 UI 重绘
  void touchPeer(String peerId) {
    final existing = _peers[peerId];
    if (existing != null) {
      _peers[peerId] = existing.copyWith(lastSeen: DateTime.now());
    }
  }

  /// 当收到 HTTP 握手或主动扫描发现节点时注册到在线列表
  void registerPeer(PeerDevice peer) {
    if (peer.id == _localDevice.id) return;
    if (peer.id == 'web-client') return;
    final existing = _peers[peer.id];
    final isNew = existing == null;
    final mergedAddresses = <String>{
      ...peer.addresses,
      if (existing != null) ...existing.addresses,
      if (existing != null &&
          existing.ip.isNotEmpty &&
          existing.ip != '0.0.0.0' &&
          existing.ip != peer.ip)
        existing.ip,
    }.toList();
    final metadataChanged = isNew ||
        existing.alias != peer.alias ||
        existing.ip != peer.ip ||
        existing.transferPort != peer.transferPort ||
        existing.os != peer.os ||
        existing.avatar != peer.avatar ||
        !listEquals(existing.addresses, mergedAddresses);
    _peers[peer.id] = peer.copyWith(
      addresses: mergedAddresses,
      lastSeen: DateTime.now(),
    );
    if (isNew) {
      TransferLogger.discovery(
        'HTTP 注册新设备: ${peer.alias} (${peer.os}) '
        '@ ${peer.endpointLabel} [${peer.shortCode}]',
      );
    }
    if (metadataChanged) {
      notifyListeners();
    }
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
      // Web 浏览器客户端与模拟器内网节点由其自身单向发起心跳轮询，宿主机不主动发起反向连接
      if (peer.deviceType == DeviceType.web || peer.transferPort <= 0) {
        continue;
      }
      if (!localIsEmulator && peer.ip.startsWith('10.0.2.')) {
        continue;
      }
      unawaited(_registerWithPeerHttp(peer));
    }
  }

  Future<void> _registerWithPeerHttp(PeerDevice peer) async {
    if (!_inFlightPeerPolls.add(peer.id)) return;
    HttpClientRequest? req;
    try {
      final uri =
          Uri.parse('http://${peer.ip}:${peer.transferPort}/api/register');
      req = await _httpClient
          .postUrl(uri)
          .timeout(const Duration(milliseconds: 1500));
      req.headers.contentType = ContentType.json;
      req.add(utf8.encode(jsonEncode(_localDevice.toJson())));
      final resp =
          await req.close().timeout(const Duration(milliseconds: 1500));
      if (resp.statusCode == HttpStatus.ok) {
        final body = await utf8.decoder
            .bind(resp)
            .join()
            .timeout(const Duration(milliseconds: 1500));
        final map = jsonDecode(body) as Map<String, dynamic>;
        // 刷新该节点的在线心跳时间并同步最新名称及多网卡地址
        final existing = _peers[peer.id];
        if (existing != null) {
          final remoteAlias = (map['alias'] as String?)?.trim();
          final nextAlias = (remoteAlias != null && remoteAlias.isNotEmpty)
              ? remoteAlias
              : existing.alias;
          final remoteAddresses = (map['addresses'] as List<dynamic>?)
                  ?.whereType<String>()
                  .map((e) => e.trim())
                  .where((e) => e.isNotEmpty && e != '0.0.0.0')
                  .toList() ??
              existing.addresses;
          final aliasChanged = nextAlias != existing.alias;
          _peers[peer.id] = existing.copyWith(
            alias: nextAlias,
            addresses: remoteAddresses,
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
      try {
        req?.abort();
      } catch (_) {}
    } finally {
      _inFlightPeerPolls.remove(peer.id);
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

  /// 通过短码（如 105 / #105）或完整 IP(:端口) 手动连接设备（原生支持 /24 及 /23 相邻网段短码定位）
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

    final candidateTargetIps = <String>[];
    // 如果只输入了 1~3 位数字短码（例如 "105" 或 "21"），自动拼接当前局域网 /24 及 /23 相邻网段前缀
    if (RegExp(r'^\d{1,3}$').hasMatch(targetIp)) {
      final shortHost = targetIp;
      for (final localIp in _localDevice.candidateIps) {
        final parts = localIp.split('.');
        if (parts.length == 4 && parts[0] != '127') {
          final same24 = '${parts[0]}.${parts[1]}.${parts[2]}.$shortHost';
          if (!candidateTargetIps.contains(same24)) {
            candidateTargetIps.add(same24);
          }
          final thirdOctet = int.tryParse(parts[2]);
          if (thirdOctet != null && thirdOctet >= 0 && thirdOctet <= 255) {
            final companion23 =
                '${parts[0]}.${parts[1]}.${thirdOctet ^ 1}.$shortHost';
            if (!candidateTargetIps.contains(companion23)) {
              candidateTargetIps.add(companion23);
            }
          }
        }
      }
      if (candidateTargetIps.isEmpty) {
        candidateTargetIps.add('127.0.0.1');
      }
    } else {
      candidateTargetIps.add(targetIp);
    }

    if (explicitPort != null) {
      for (final ip in candidateTargetIps) {
        final found = await _probeSingleEndpoint(ip, explicitPort);
        if (found != null) return found;
      }
      return null;
    }

    // 默认先在候选网段探测标准端口，再探测相邻备用端口
    for (final ip in candidateTargetIps) {
      final found = await _probeSingleEndpoint(ip, defaultTransferPort);
      if (found != null) return found;
    }
    for (final ip in candidateTargetIps) {
      for (int port = defaultTransferPort + 1;
          port <= defaultTransferPort + 5;
          port++) {
        final found = await _probeSingleEndpoint(ip, port);
        if (found != null) return found;
      }
    }
    return null;
  }

  /// 快速扫描当前 /24 及 /23 邻近子网内所有在线节点（应对开启了 UDP 广播隔离的路由器）
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
        final prefixes = <String>['${parts[0]}.${parts[1]}.${parts[2]}'];
        final thirdOctet = int.tryParse(parts[2]);
        if (thirdOctet != null && thirdOctet >= 0 && thirdOctet <= 255) {
          final companionPrefix = '${parts[0]}.${parts[1]}.${thirdOctet ^ 1}';
          if (!prefixes.contains(companionPrefix)) {
            prefixes.add(companionPrefix);
          }
        }

        const batchSize = 48;
        for (final prefix in prefixes) {
          for (int start = 1; start <= 254; start += batchSize) {
            final futures = <Future<PeerDevice?>>[];
            for (int i = start; i < start + batchSize && i <= 254; i++) {
              final candidateIp = '$prefix.$i';
              if (_localDevice.candidateIps.contains(candidateIp)) continue;
              futures.add(
                _probeSingleEndpoint(
                  candidateIp,
                  defaultTransferPort,
                  timeout: const Duration(milliseconds: 300),
                ),
              );
            }
            final results = await Future.wait(futures);
            discoveredCount += results.whereType<PeerDevice>().length;
          }
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

    final client = HttpClient()
      ..findProxy = null
      ..connectionTimeout = timeout;
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

  void _removeStalePeers({bool busy = false}) {
    final now = DateTime.now();
    final effectiveTimeout = busy ? busyPeerTimeout : peerTimeout;
    bool removed = false;
    _peers.removeWhere((_, peer) {
      final expired = now.difference(peer.lastSeen) > effectiveTimeout;
      if (expired) removed = true;
      return expired;
    });
    if (removed) {
      notifyListeners();
    }
  }

  Future<void> handleAppLifecycleResume() async {
    final updated = await detectLocalIpAddresses();
    if (updated.primaryIp != _localDevice.ip ||
        !listEquals(updated.allIps, _localDevice.addresses)) {
      _localDevice = _localDevice.copyWith(
        ip: updated.primaryIp,
        addresses: updated.allIps,
        lastSeen: DateTime.now(),
      );
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
    _httpClient.close(force: true);
    super.dispose();
  }
}
