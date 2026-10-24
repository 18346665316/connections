import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';
import 'package:flutter/foundation.dart';
import '../models/peer_device.dart';
import '../models/transfer_item.dart';
import '../utils/transfer_logger.dart';
import 'discovery_service.dart';
import 'settings_service.dart';

typedef IncomingRequestResolver = Future<bool> Function(TransferSession session);

class _OutboundPullContext {
  final TransferSession session;
  final String peerId;
  final String token;
  bool offerDelivered = false;
  final Completer<bool> decisionCompleter = Completer<bool>();
  final Completer<void> completionCompleter = Completer<void>();
  int completedFilesCount = 0;
  final Stopwatch stopwatch = Stopwatch();
  int lastNotifyMs = 0;
  int lastLogMs = 0;

  _OutboundPullContext({
    required this.session,
    required this.peerId,
    required this.token,
  });
}

class TransferService extends ChangeNotifier {
  static const int _maxControlBodyBytes = 1024 * 1024; // 1 MB 控制信令上限

  final DiscoveryService discoveryService;
  HttpServer? _server;
  int _boundPort = DiscoveryService.defaultTransferPort;
  bool _autoAccept = false;
  late String _saveDirectoryPath;
  final Set<String> _trustedPeerIds = {};

  IncomingRequestResolver? onIncomingTransferRequest;
  ValueChanged<String>? onSessionCancelled;

  final List<TransferSession> _sessions = [];
  final Map<String, TransferSession> _activeReceivingSessions = {};
  final Map<String, Stopwatch> _receivingSessionStopwatches = {};
  final Map<String, String> _sessionTokens = {};
  final Map<String, Completer<bool>> _pendingConfirmations = {};
  final Map<String, HttpClient> _activeSendingClients = {};
  final Map<String, ({String ip, int port, String remoteSessionId})>
      _activeSendingRemotes = {};
  final Map<String, _OutboundPullContext> _outboundPullSessions = {};
  final Set<String> _processedPullOfferIds = {};
  final Set<String> _cancelledSessionIds = {};

  int get boundPort => _boundPort;
  bool get autoAccept => _autoAccept;
  String get saveDirectoryPath => _saveDirectoryPath;
  int get trustedPeerCount => _trustedPeerIds.length;
  List<TransferSession> get sessions => List.unmodifiable(_sessions);

  TransferService({required this.discoveryService}) {
    discoveryService.onPullOffersReceived = _handleIncomingPullOffers;
    final persisted = SettingsService.load();
    _autoAccept = persisted.autoAccept;
    _trustedPeerIds.addAll(persisted.trustedPeerIds);
    if (persisted.saveDirectoryPath != null &&
        persisted.saveDirectoryPath!.trim().isNotEmpty) {
      final dir = Directory(persisted.saveDirectoryPath!.trim());
      if (_isDirectoryWritable(dir)) {
        _saveDirectoryPath = dir.path;
        _cleanStalePartFiles();
        return;
      }
    }
    _saveDirectoryPath = _resolveDefaultSaveDirectory();
    _cleanStalePartFiles();
  }

  bool isPeerTrusted(String peerId) => _trustedPeerIds.contains(peerId);

  void toggleTrustPeer(String peerId, {bool? trust}) {
    final shouldTrust = trust ?? !_trustedPeerIds.contains(peerId);
    if (shouldTrust) {
      _trustedPeerIds.add(peerId);
    } else {
      _trustedPeerIds.remove(peerId);
    }
    final current = SettingsService.load();
    unawaited(
      SettingsService.save(
        current.copyWith(trustedPeerIds: Set<String>.from(_trustedPeerIds)),
      ),
    );
    notifyListeners();
  }

  void clearTrustedPeers() {
    if (_trustedPeerIds.isEmpty) return;
    _trustedPeerIds.clear();
    final current = SettingsService.load();
    unawaited(
      SettingsService.save(
        current.copyWith(trustedPeerIds: const {}),
      ),
    );
    notifyListeners();
  }

  /// 清理异常断电或强退遗留的 .part 临时文件
  void _cleanStalePartFiles() {
    try {
      final dir = Directory(_saveDirectoryPath);
      if (!dir.existsSync()) return;
      for (final entity in dir.listSync(recursive: true, followLinks: false)) {
        if (entity is File && entity.path.endsWith('.part')) {
          try {
            entity.deleteSync();
          } catch (_) {}
        }
      }
    } catch (_) {}
  }

  void setAutoAccept(bool value) {
    _autoAccept = value;
    final current = SettingsService.load();
    unawaited(SettingsService.save(current.copyWith(autoAccept: value)));
    notifyListeners();
  }

  bool setSaveDirectoryPath(String path) {
    final trimmed = path.trim();
    if (trimmed.isEmpty) return false;
    try {
      final dir = Directory(trimmed);
      if (!_isDirectoryWritable(dir)) {
        return false;
      }
      _saveDirectoryPath = dir.path;
      final current = SettingsService.load();
      unawaited(
        SettingsService.save(current.copyWith(saveDirectoryPath: dir.path)),
      );
      notifyListeners();
      return true;
    } catch (_) {
      return false;
    }
  }

  /// 探测目录在当前系统沙盒策略下（如 Android 11+ Scoped Storage 或 iOS 沙盒）是否真正可读写
  bool _isDirectoryWritable(Directory dir) {
    try {
      if (!dir.existsSync()) {
        dir.createSync(recursive: true);
      }
      final probe = File(
        '${dir.path}${Platform.pathSeparator}.probe_${DateTime.now().microsecondsSinceEpoch}_${Random().nextInt(9999)}',
      );
      probe.writeAsBytesSync(const [1], flush: true);
      probe.deleteSync();
      return true;
    } catch (_) {
      return false;
    }
  }

  String _resolveDefaultSaveDirectory() {
    try {
      if (Platform.isAndroid) {
        final candidates = <Directory>[
          Directory('/storage/emulated/0/Download/Connections'),
          Directory(
            '${Directory.systemTemp.parent.path}${Platform.pathSeparator}files${Platform.pathSeparator}Connections',
          ),
        ];
        for (final dir in candidates) {
          if (_isDirectoryWritable(dir)) {
            return dir.path;
          }
        }
      }

      final home = Platform.environment['HOME'] ??
          Platform.environment['USERPROFILE'] ??
          Directory.systemTemp.path;

      if (Platform.isIOS) {
        // iOS 存储在 ~/Documents/Connections，配合 Info.plist UIFileSharingEnabled 可直接在系统「文件」App 访问
        final iosDocsDir =
            Directory('$home${Platform.pathSeparator}Documents${Platform.pathSeparator}Connections');
        if (_isDirectoryWritable(iosDocsDir)) {
          return iosDocsDir.path;
        }
      }

      final downloadsDir = Directory('$home${Platform.pathSeparator}Downloads');
      if (downloadsDir.existsSync()) {
        final target = Directory(
          '${downloadsDir.path}${Platform.pathSeparator}Connections',
        );
        if (_isDirectoryWritable(target)) {
          return target.path;
        }
      }
    } catch (_) {}
    final fallback = Directory(
      '${Directory.systemTemp.path}${Platform.pathSeparator}Connections',
    );
    if (!fallback.existsSync()) {
      fallback.createSync(recursive: true);
    }
    return fallback.path;
  }

  String _generateSecureToken() {
    final rng = Random.secure();
    final bytes = List<int>.generate(16, (_) => rng.nextInt(256));
    return base64UrlEncode(bytes);
  }

  /// 带大小上限的 JSON 请求体读取（防止超大恶意报文耗尽内存）
  Future<Map<String, dynamic>> _readJsonBodyLimited(HttpRequest request) async {
    final builder = BytesBuilder(copy: false);
    await for (final chunk in request) {
      if (builder.length + chunk.length > _maxControlBodyBytes) {
        throw const HttpException('控制请求体超出大小限制 (Max 1MB)');
      }
      builder.add(chunk);
    }
    final text = utf8.decode(builder.takeBytes());
    return jsonDecode(text) as Map<String, dynamic>;
  }

  Future<int> startServer() async {
    for (int port = DiscoveryService.defaultTransferPort;
        port <= DiscoveryService.defaultTransferPort + 15;
        port++) {
      try {
        _server = await HttpServer.bind(InternetAddress.anyIPv4, port);
        _boundPort = port;
        break;
      } catch (_) {
        continue;
      }
    }

    _server ??= await HttpServer.bind(InternetAddress.anyIPv4, 0);
    _server!.autoCompress = false; // 禁用二进制流无意义的 GZip 压缩开销，最大化局域网吞吐
    _boundPort = _server!.port;

    _server!.listen(_handleHttpRequest, onError: (Object e) {
      TransferLogger.error('Server', 'HTTP P2P 服务异常: $e');
    });

    TransferLogger.recv(
      'P2P 传输服务已监听端口: $_boundPort | 默认接收目录: $_saveDirectoryPath',
    );
    notifyListeners();
    return _boundPort;
  }

  Future<void> _handleHttpRequest(HttpRequest request) async {
    request.response.headers.set('Access-Control-Allow-Origin', '*');
    request.response.headers
        .set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    request.response.headers.set(
      'Access-Control-Allow-Headers',
      'Content-Type, X-Peer-Info, X-Upload-Token',
    );

    if (request.method == 'OPTIONS') {
      request.response.statusCode = HttpStatus.noContent;
      await request.response.close();
      return;
    }

    final path = request.uri.path;
    try {
      if (path == '/api/info' && request.method == 'GET') {
        await _handleGetInfo(request);
      } else if (path == '/api/register' && request.method == 'POST') {
        await _handlePostRegister(request);
      } else if (path == '/api/prepare-upload' && request.method == 'POST') {
        await _handlePrepareUpload(request);
      } else if (path == '/api/upload' && request.method == 'POST') {
        await _handleFileUploadStream(request);
      } else if (path == '/api/pull-decision' && request.method == 'POST') {
        await _handlePullDecision(request);
      } else if (path == '/api/pull-file' && request.method == 'GET') {
        await _handlePullFileStream(request);
      } else if (path == '/api/cancel' && request.method == 'POST') {
        await _handleCancelSession(request);
      } else if (path == '/' && request.method == 'GET') {
        await _handleWebPortal(request);
      } else {
        request.response.statusCode = HttpStatus.notFound;
        request.response.write('Not Found');
        await request.response.close();
      }
    } catch (e) {
      try {
        request.response.statusCode = HttpStatus.internalServerError;
        request.response.write(jsonEncode({'error': e.toString()}));
        await request.response.close();
      } catch (_) {}
    }
  }

  PeerDevice _resolveRemotePeer(Map<String, dynamic> map, String remoteIp) {
    final parsed = PeerDevice.fromJson(map, fallbackIp: remoteIp);
    final isLoopbackOrSelf = remoteIp == '127.0.0.1' ||
        remoteIp == '::1' ||
        remoteIp == '0.0.0.0' ||
        remoteIp == discoveryService.localDevice.ip;
    return isLoopbackOrSelf ? parsed : parsed.copyWith(ip: remoteIp);
  }

  Future<void> _handleGetInfo(HttpRequest request) async {
    final peerHeader = request.headers.value('X-Peer-Info');
    final remoteIp =
        request.connectionInfo?.remoteAddress.address ?? '127.0.0.1';
    if (peerHeader != null && peerHeader.isNotEmpty) {
      try {
        final decoded = utf8.decode(base64Decode(peerHeader));
        final map = jsonDecode(decoded) as Map<String, dynamic>;
        final peer = _resolveRemotePeer(map, remoteIp);
        discoveryService.registerPeer(peer);
      } catch (_) {}
    }

    request.response.headers.contentType = ContentType.json;
    request.response.write(jsonEncode(discoveryService.localDevice.toJson()));
    await request.response.close();
  }

  Future<void> _handlePostRegister(HttpRequest request) async {
    final remoteIp =
        request.connectionInfo?.remoteAddress.address ?? '127.0.0.1';
    final map = await _readJsonBodyLimited(request);
    final peer = _resolveRemotePeer(map, remoteIp);
    discoveryService.registerPeer(peer);

    // 检查是否有挂起发往该节点的反向拉取任务（用于穿透 Android 模拟器 10.0.2.16 等单向 NAT）
    final pendingOffers = <Map<String, dynamic>>[];
    for (final ctx in _outboundPullSessions.values) {
      if (ctx.peerId == peer.id &&
          !ctx.offerDelivered &&
          !_cancelledSessionIds.contains(ctx.session.sessionId)) {
        ctx.offerDelivered = true;
        pendingOffers.add({
          'sessionId': ctx.session.sessionId,
          'token': ctx.token,
          'sender': discoveryService.localDevice.toJson(),
          'files': ctx.session.files.map((f) => f.toJson()).toList(),
          'textMessage': ctx.session.textMessage,
        });
      }
    }

    final responseMap = <String, dynamic>{
      ...discoveryService.localDevice.toJson(),
      if (pendingOffers.isNotEmpty) 'pullOffers': pendingOffers,
    };

    request.response.headers.contentType = ContentType.json;
    request.response.write(jsonEncode(responseMap));
    await request.response.close();
  }

  Future<void> _handleCancelSession(HttpRequest request) async {
    final sessionId = request.uri.queryParameters['sessionId'] ?? '';
    await request.drain<void>();
    if (sessionId.isNotEmpty) {
      _markSessionCancelledInternal(sessionId);
    }
    request.response.headers.contentType = ContentType.json;
    request.response.write(jsonEncode({'ok': true}));
    await request.response.close();
  }

  void _markSessionCancelledInternal(String sessionId) {
    _cancelledSessionIds.add(sessionId);
    _sessionTokens.remove(sessionId);
    _receivingSessionStopwatches.remove(sessionId);

    final pending = _pendingConfirmations.remove(sessionId);
    if (pending != null && !pending.isCompleted) {
      pending.complete(false);
    }

    final pullCtx = _outboundPullSessions.remove(sessionId);
    if (pullCtx != null) {
      if (!pullCtx.decisionCompleter.isCompleted) {
        pullCtx.decisionCompleter.complete(false);
      }
      if (!pullCtx.completionCompleter.isCompleted) {
        pullCtx.completionCompleter.complete();
      }
    }

    final activeReceiving = _activeReceivingSessions.remove(sessionId);
    if (activeReceiving != null) {
      activeReceiving.status = TransferStatus.cancelled;
    }

    for (final s in _sessions) {
      if (s.sessionId == sessionId && s.isActive) {
        s.status = TransferStatus.cancelled;
      }
    }

    onSessionCancelled?.call(sessionId);
    notifyListeners();
  }

  /// 主动取消正在发送或正在接收的传输会话
  Future<void> cancelSession(String sessionId) async {
    TransferLogger.log('Cancel', '用户主动取消传输会话: $sessionId');
    final targetSession = _sessions
        .where((s) => s.sessionId == sessionId)
        .firstOrNull;
    _markSessionCancelledInternal(sessionId);

    final sendingClient = _activeSendingClients.remove(sessionId);
    sendingClient?.close(force: true);

    final remote = _activeSendingRemotes.remove(sessionId);
    final notifyIp = remote?.ip ?? targetSession?.targetPeer?.ip;
    final notifyPort = remote?.port ?? targetSession?.targetPeer?.transferPort;
    final notifySessionId = remote?.remoteSessionId ?? sessionId;

    if (notifyIp != null &&
        notifyPort != null &&
        notifyPort > 0 &&
        notifySessionId.isNotEmpty) {
      final notifyClient = HttpClient()
        ..connectionTimeout = const Duration(seconds: 2);
      try {
        final uri = Uri.parse(
          'http://$notifyIp:$notifyPort/api/cancel?sessionId=${Uri.encodeComponent(notifySessionId)}',
        );
        final req = await notifyClient.postUrl(uri);
        final resp = await req.close();
        await resp.drain<void>();
      } catch (_) {
      } finally {
        notifyClient.close(force: true);
      }
    }
  }

  Future<void> _handlePrepareUpload(HttpRequest request) async {
    final remoteIp =
        request.connectionInfo?.remoteAddress.address ?? '127.0.0.1';
    final map = await _readJsonBodyLimited(request);

    final senderMap = map['sender'] as Map<String, dynamic>? ?? {};
    final sender = _resolveRemotePeer(senderMap, remoteIp);
    discoveryService.registerPeer(sender);

    final rawFiles = (map['files'] as List<dynamic>?) ?? [];
    final files = rawFiles
        .map((e) => TransferFileItem.fromJson(e as Map<String, dynamic>))
        .toList();
    final textMessage = map['textMessage'] as String?;
    final clientSessionId = (map['clientSessionId'] as String?)?.trim();

    final totalBytes = files.fold<int>(0, (sum, item) => sum + item.size);
    final sessionId = (clientSessionId != null && clientSessionId.isNotEmpty)
        ? clientSessionId
        : '${DateTime.now().millisecondsSinceEpoch}-${Random().nextInt(99999)}';

    final session = TransferSession(
      sessionId: sessionId,
      peerId: sender.id,
      peerAlias: sender.alias,
      peerAvatar: sender.avatar,
      peerIp: sender.ip,
      targetPeer: sender,
      direction: TransferDirection.receiving,
      files: files,
      textMessage: textMessage,
      totalBytes: totalBytes,
      status: TransferStatus.waitingConfirmation,
    );

    _sessions.insert(0, session);
    notifyListeners();

    TransferLogger.recv(
      '收到传输握手 <- 来自: ${sender.alias} (${sender.ip}) '
      '| 会话: $sessionId | 文件数: ${files.length} '
      '| 总大小: ${TransferSession.formatBytes(totalBytes)}'
      '${textMessage != null ? " | 附带文本: ${textMessage.length}字" : ""}',
    );

    final isTrusted = _trustedPeerIds.contains(sender.id);
    bool accepted = _autoAccept || isTrusted;
    if (accepted) {
      TransferLogger.recv(
        '自动同意接收 [$sessionId] (原因: ${isTrusted ? "已信任设备" : "开启了自动接收"})',
      );
    } else if (onIncomingTransferRequest != null) {
      final completer = Completer<bool>();
      _pendingConfirmations[sessionId] = completer;

      unawaited(
        onIncomingTransferRequest!(session).then((decision) {
          if (!completer.isCompleted) {
            completer.complete(decision);
          }
        }).catchError((Object _) {
          if (!completer.isCompleted) {
            completer.complete(false);
          }
        }),
      );

      accepted = await completer.future.timeout(
        const Duration(seconds: 60),
        onTimeout: () {
          onSessionCancelled?.call(sessionId);
          return false;
        },
      );
      _pendingConfirmations.remove(sessionId);
    } else if (!accepted && onIncomingTransferRequest == null) {
      accepted = true;
    }

    if (!accepted || _cancelledSessionIds.contains(sessionId)) {
      session.status = _cancelledSessionIds.contains(sessionId)
          ? TransferStatus.cancelled
          : TransferStatus.rejected;
      TransferLogger.recv(
        '传输请求未通过 [$sessionId] -> 状态: ${session.status.name}',
      );
      notifyListeners();
      request.response.statusCode = HttpStatus.forbidden;
      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode({'accepted': false}));
      await request.response.close();
      return;
    }

    final uploadToken = _generateSecureToken();

    if (files.isEmpty && textMessage != null) {
      session.status = TransferStatus.completed;
      TransferLogger.recv(
        '✅ 文本消息接收完成 [$sessionId] <- 来自 ${sender.alias}: "$textMessage"',
      );
      notifyListeners();
      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode({
        'accepted': true,
        'sessionId': sessionId,
        'token': uploadToken,
      }));
      await request.response.close();
      return;
    }

    session.status = TransferStatus.transferring;
    _activeReceivingSessions[sessionId] = session;
    _receivingSessionStopwatches[sessionId] = Stopwatch()..start();
    _sessionTokens[sessionId] = uploadToken;
    notifyListeners();

    request.response.headers.contentType = ContentType.json;
    request.response.write(jsonEncode({
      'accepted': true,
      'sessionId': sessionId,
      'token': uploadToken,
    }));
    await request.response.close();
  }

  Future<void> _handleFileUploadStream(HttpRequest request) async {
    final sessionId = request.uri.queryParameters['sessionId'] ?? '';
    final fileId = request.uri.queryParameters['fileId'] ?? '';
    final token = request.headers.value('X-Upload-Token') ??
        request.uri.queryParameters['token'] ??
        '';
    final session = _activeReceivingSessions[sessionId];
    final expectedToken = _sessionTokens[sessionId];

    if (session == null ||
        _cancelledSessionIds.contains(sessionId) ||
        expectedToken == null ||
        token != expectedToken) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.unauthorized;
      request.response.write(jsonEncode({'error': '未授权或已失效的传输会话'}));
      await request.response.close();
      return;
    }

    // 严格校验 fileId 必须在已批准的会话文件清单内
    final matchingFiles = session.files.where((f) => f.id == fileId);
    if (matchingFiles.isEmpty) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.badRequest;
      request.response.write(jsonEncode({'error': '未声明的文件 ID'}));
      await request.response.close();
      return;
    }
    final fileItem = matchingFiles.first;

    final saveDir = Directory(_saveDirectoryPath);
    if (!saveDir.existsSync()) {
      saveDir.createSync(recursive: true);
    }

    final finalFile = _resolveTargetFile(saveDir.path, fileItem);
    final partFile = File('${finalFile.path}.part');
    final sink = partFile.openWrite();

    final fileStopwatch = Stopwatch()..start();
    final sessionStopwatch =
        _receivingSessionStopwatches[sessionId] ?? fileStopwatch;
    int lastNotifyMs = 0;
    int lastLogMs = 0;
    int bytesInCurrentFile = 0;

    TransferLogger.recv(
      '开始接收文件 [$sessionId] -> ${fileItem.relativePath ?? fileItem.name} '
      '(${TransferSession.formatBytes(fileItem.size)})',
    );

    try {
      await for (final chunk in request) {
        if (_cancelledSessionIds.contains(sessionId)) {
          throw const SocketException('用户已取消接收');
        }
        bytesInCurrentFile += chunk.length;
        if (fileItem.size > 0 && bytesInCurrentFile > fileItem.size) {
          throw const HttpException('接收字节数超出声明的文件大小');
        }
        sink.add(chunk);
        session.transferredBytes += chunk.length;

        final elapsedMs = sessionStopwatch.elapsedMilliseconds;
        if (elapsedMs - lastNotifyMs >= 100) {
          lastNotifyMs = elapsedMs;
          if (elapsedMs > 0) {
            session.speedBytesPerSec =
                (session.transferredBytes * 1000) / elapsedMs;
          }
          notifyListeners();
        }
        if (elapsedMs - lastLogMs >= 500) {
          lastLogMs = elapsedMs;
          final pct = (session.progress * 100).toStringAsFixed(1);
          TransferLogger.recv(
            '接收进度 [$sessionId] ${fileItem.name}: $pct% '
            '(${TransferSession.formatBytes(session.transferredBytes)} / ${TransferSession.formatBytes(session.totalBytes)}) '
            '| 速率: ${TransferSession.formatSpeed(session.speedBytesPerSec)}',
          );
        }
      }
      await sink.flush();
      await sink.close();

      // 严格校验文件完整性（防止发送端网络闪断导致半截文件被当作完整文件保存）
      if (fileItem.size > 0 && bytesInCurrentFile != fileItem.size) {
        throw HttpException(
          '文件完整性校验失败：预期 ${fileItem.size} B，实际接收 $bytesInCurrentFile B',
        );
      }

      // 原子重命名 .part 临时文件为最终文件（若原定目标路径未被并发任务抢占则直接使用，避免因自身 .part 存在而误追加 (1)）
      final resolvedFinal = finalFile.existsSync()
          ? _resolveTargetFile(saveDir.path, fileItem)
          : finalFile;
      await partFile.rename(resolvedFinal.path);

      final fileElapsedMs = max(1, fileStopwatch.elapsedMilliseconds);
      final sessionElapsedMs = max(1, sessionStopwatch.elapsedMilliseconds);
      session.speedBytesPerSec =
          (session.transferredBytes * 1000) / sessionElapsedMs;

      TransferLogger.recv(
        '文件落盘完成 [$sessionId]: ${resolvedFinal.path} '
        '| 大小: ${TransferSession.formatBytes(bytesInCurrentFile)} '
        '| 耗时: ${fileElapsedMs}ms '
        '| 速率: ${TransferSession.formatSpeed(session.speedBytesPerSec)}',
      );

      session.savedPaths.add(resolvedFinal.path);
      if (session.savedPaths.length >= session.files.length) {
        session.status = TransferStatus.completed;
        session.transferredBytes = session.totalBytes;
        _activeReceivingSessions.remove(sessionId);
        _receivingSessionStopwatches.remove(sessionId);
        _sessionTokens.remove(sessionId);
        TransferLogger.recv(
          '✅ 会话全部接收完成 [$sessionId] | 共 ${session.savedPaths.length} 个文件 '
          '(${TransferSession.formatBytes(session.totalBytes)})',
        );
      }
      notifyListeners();

      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode({
        'ok': true,
        'savedPath': resolvedFinal.path,
      }));
      await request.response.close();
    } catch (e) {
      await sink.close();
      try {
        if (partFile.existsSync()) {
          partFile.deleteSync();
        }
      } catch (_) {}

      if (_cancelledSessionIds.contains(sessionId)) {
        session.status = TransferStatus.cancelled;
        TransferLogger.recv('⚠️ 接收已取消 [$sessionId]: ${fileItem.name}');
      } else {
        session.status = TransferStatus.failed;
        session.errorMessage = e.toString();
        TransferLogger.error('Recv', '接收失败 [$sessionId] (${fileItem.name}): $e');
      }
      _activeReceivingSessions.remove(sessionId);
      _receivingSessionStopwatches.remove(sessionId);
      _sessionTokens.remove(sessionId);
      notifyListeners();
      rethrow;
    }
  }

  File _resolveTargetFile(String baseDirPath, TransferFileItem item) {
    if (item.relativePath != null && item.relativePath!.trim().isNotEmpty) {
      final rawSegments = item.relativePath!
          .replaceAll('\\', '/')
          .split('/')
          .where((seg) => seg.isNotEmpty && seg != '.' && seg != '..')
          .map((seg) => seg.replaceAll(RegExp(r'[<>:"/\\|?*\x00-\x1F]'), '_'))
          .toList();

      if (rawSegments.length > 1) {
        final subDirs = rawSegments
            .sublist(0, rawSegments.length - 1)
            .join(Platform.pathSeparator);
        final targetSubDir =
            Directory('$baseDirPath${Platform.pathSeparator}$subDirs');
        if (!targetSubDir.existsSync()) {
          targetSubDir.createSync(recursive: true);
        }
        return _resolveUniqueFile(targetSubDir.path, rawSegments.last);
      }
    }
    return _resolveUniqueFile(baseDirPath, item.name);
  }

  File _resolveUniqueFile(String dirPath, String rawFileName) {
    final safeName =
        rawFileName.replaceAll(RegExp(r'[<>:"/\\|?*\x00-\x1F]'), '_');
    var candidate = File('$dirPath${Platform.pathSeparator}$safeName');
    if (!candidate.existsSync() && !File('${candidate.path}.part').existsSync()) {
      return candidate;
    }

    final dotIndex = safeName.lastIndexOf('.');
    final baseName =
        dotIndex > 0 ? safeName.substring(0, dotIndex) : safeName;
    final ext = dotIndex > 0 ? safeName.substring(dotIndex) : '';

    int counter = 1;
    while (candidate.existsSync() ||
        File('${candidate.path}.part').existsSync()) {
      candidate =
          File('$dirPath${Platform.pathSeparator}$baseName ($counter)$ext');
      counter++;
    }
    return candidate;
  }

  /// 向指定局域网设备发送文件列表（具备 TCP 背压流控与令牌鉴权）
  Future<TransferSession> sendFilesToPeer({
    required PeerDevice peer,
    required List<TransferFileItem> files,
    String? textMessage,
  }) async {
    final totalBytes = files.fold<int>(0, (sum, f) => sum + f.size);
    final localSessionId =
        'out-${DateTime.now().millisecondsSinceEpoch}-${Random().nextInt(99999)}';

    final session = TransferSession(
      sessionId: localSessionId,
      peerId: peer.id,
      peerAlias: peer.alias,
      peerAvatar: peer.avatar,
      peerIp: peer.ip,
      targetPeer: peer,
      direction: TransferDirection.sending,
      files: files,
      textMessage: textMessage,
      totalBytes: totalBytes,
      status: TransferStatus.waitingConfirmation,
    );

    _sessions.insert(0, session);
    notifyListeners();

    TransferLogger.send(
      '发起传输请求 -> 目标: ${peer.alias} (${peer.ip}:${peer.transferPort}) '
      '| 会话: $localSessionId | 文件数: ${files.length} '
      '| 总大小: ${TransferSession.formatBytes(totalBytes)}'
      '${textMessage != null ? " | 附带文本: ${textMessage.length}字" : ""}',
    );

    // 若目标处于 Android 模拟器 NAT 网段 (10.0.2.x) 且本机是宿主机，直接走反向拉取通道（避免宿主机直连 10.0.2.16 被 RST）
    final isTargetEmulatorNat = peer.ip.startsWith('10.0.2.') &&
        !discoveryService.localDevice.ip.startsWith('10.0.2.');
    if (isTargetEmulatorNat) {
      TransferLogger.send(
        '目标 ${peer.alias} (${peer.ip}) 位于模拟器/NAT网段，自动启用反向拉取 (Reverse-Pull) 通道...',
      );
      return _sendViaReversePull(
        peer: peer,
        session: session,
      );
    }

    final client = HttpClient()
      ..connectionTimeout = const Duration(seconds: 3)
      ..idleTimeout = const Duration(seconds: 30)
      ..maxConnectionsPerHost = 6
      ..autoUncompress = false;
    _activeSendingClients[localSessionId] = client;
    _activeSendingRemotes[localSessionId] = (
      ip: peer.ip,
      port: peer.transferPort,
      remoteSessionId: localSessionId,
    );

    try {
      final prepUri =
          Uri.parse('http://${peer.ip}:${peer.transferPort}/api/prepare-upload');
      HttpClientRequest prepReq;
      HttpClientResponse prepResp;
      try {
        prepReq = await client.postUrl(prepUri);
        prepReq.headers.contentType = ContentType.json;
        prepReq.add(utf8.encode(jsonEncode({
          'clientSessionId': localSessionId,
          'sender': discoveryService.localDevice.toJson(),
          'files': files.map((f) => f.toJson()).toList(),
          'textMessage': textMessage,
        })));
        prepResp = await prepReq.close();
      } on Exception catch (connErr) {
        if (_cancelledSessionIds.contains(localSessionId)) rethrow;
        TransferLogger.send(
          '直连 ${peer.ip}:${peer.transferPort} 不可达 ($connErr)，自动切换为反向拉取 (Reverse-Pull) 通道...',
        );
        return await _sendViaReversePull(
          peer: peer,
          session: session,
        );
      }
      final prepBody = await utf8.decoder.bind(prepResp).join();

      if (_cancelledSessionIds.contains(localSessionId)) {
        session.status = TransferStatus.cancelled;
        notifyListeners();
        return session;
      }

      if (prepResp.statusCode == HttpStatus.forbidden) {
        session.status = TransferStatus.rejected;
        TransferLogger.send('⚠️ 对方 (${peer.alias}) 拒绝了传输请求 [$localSessionId]');
        notifyListeners();
        return session;
      }
      if (prepResp.statusCode != HttpStatus.ok) {
        throw Exception('对方节点响应异常 (${prepResp.statusCode}): $prepBody');
      }

      final prepJson = jsonDecode(prepBody) as Map<String, dynamic>;
      final remoteSessionId =
          prepJson['sessionId'] as String? ?? localSessionId;
      final uploadToken = prepJson['token'] as String? ?? '';

      _activeSendingRemotes[localSessionId] = (
        ip: peer.ip,
        port: peer.transferPort,
        remoteSessionId: remoteSessionId,
      );

      if (files.isEmpty) {
        session.status = TransferStatus.completed;
        TransferLogger.send('✅ 文本消息已送达 ${peer.alias} [$localSessionId]');
        notifyListeners();
        return session;
      }

      session.status = TransferStatus.transferring;
      notifyListeners();

      final stopwatch = Stopwatch()..start();
      int lastNotifyMs = 0;
      int lastLogMs = 0;

      // 多文件并发流水线传输（Keep-Alive 连接复用，最多 4 路并发消除海量小文件 RTT 延迟）
      final concurrency = files.length > 1 ? min(4, files.length) : 1;
      TransferLogger.send(
        '对方已接受 [$localSessionId]，开始流式上传 (并发通道数: $concurrency)...',
      );
      int nextFileIndex = 0;

      Future<void> uploadWorker() async {
        while (true) {
          if (_cancelledSessionIds.contains(localSessionId)) {
            return;
          }
          final idx = nextFileIndex++;
          if (idx >= files.length) return;
          final fileItem = files[idx];

          TransferLogger.send(
            '上传文件 [${idx + 1}/${files.length}]: ${fileItem.relativePath ?? fileItem.name} '
            '(${TransferSession.formatBytes(fileItem.size)})',
          );

          final uploadUri = Uri.parse(
            'http://${peer.ip}:${peer.transferPort}/api/upload'
            '?sessionId=${Uri.encodeComponent(remoteSessionId)}'
            '&fileId=${Uri.encodeComponent(fileItem.id)}'
            '&fileName=${Uri.encodeComponent(fileItem.name)}',
          );

          final uploadReq = await client.postUrl(uploadUri);
          uploadReq.headers.contentType = ContentType.binary;
          uploadReq.headers.set('X-Upload-Token', uploadToken);
          uploadReq.bufferOutput = false; // 禁用额外输出缓冲，直通 Socket
          uploadReq.contentLength = fileItem.size;

          final rawStream = _openFileOrMemoryStream(fileItem);
          final trackedStream = rawStream.transform<List<int>>(
            StreamTransformer.fromHandlers(
              handleData: (chunk, sink) {
                if (_cancelledSessionIds.contains(localSessionId)) {
                  sink.addError(const SocketException('用户已取消发送'));
                  return;
                }
                sink.add(chunk);
                session.transferredBytes += chunk.length;

                final elapsedMs = stopwatch.elapsedMilliseconds;
                if (elapsedMs - lastNotifyMs >= 100) {
                  lastNotifyMs = elapsedMs;
                  if (elapsedMs > 0) {
                    session.speedBytesPerSec =
                        (session.transferredBytes * 1000) / elapsedMs;
                  }
                  notifyListeners();
                }
                if (elapsedMs - lastLogMs >= 500) {
                  lastLogMs = elapsedMs;
                  final pct = (session.progress * 100).toStringAsFixed(1);
                  TransferLogger.send(
                    '发送进度 [$localSessionId]: $pct% '
                    '(${TransferSession.formatBytes(session.transferredBytes)} / ${TransferSession.formatBytes(session.totalBytes)}) '
                    '| 速率: ${TransferSession.formatSpeed(session.speedBytesPerSec)}',
                  );
                }
              },
            ),
          );

          await uploadReq.addStream(trackedStream);

          final uploadResp = await uploadReq.close();
          await uploadResp.drain<void>();
          if (uploadResp.statusCode != HttpStatus.ok) {
            throw Exception(
              '文件 ${fileItem.name} 传输失败 (${uploadResp.statusCode})',
            );
          }
        }
      }

      await Future.wait(List.generate(concurrency, (_) => uploadWorker()));

      if (_cancelledSessionIds.contains(localSessionId)) {
        session.status = TransferStatus.cancelled;
        TransferLogger.send('⚠️ 发送已取消 [$localSessionId]');
        notifyListeners();
        return session;
      }

      final totalElapsedMs = max(1, stopwatch.elapsedMilliseconds);
      session.speedBytesPerSec =
          (session.transferredBytes * 1000) / totalElapsedMs;
      session.status = TransferStatus.completed;
      session.transferredBytes = session.totalBytes;
      TransferLogger.send(
        '✅ 发送全部完成 [$localSessionId] -> ${peer.alias} '
        '| 共 ${files.length} 个文件 (${TransferSession.formatBytes(session.totalBytes)}) '
        '| 总耗时: ${totalElapsedMs}ms '
        '| 平均速率: ${TransferSession.formatSpeed(session.speedBytesPerSec)}',
      );
      notifyListeners();
      return session;
    } catch (e) {
      if (_cancelledSessionIds.contains(localSessionId) ||
          session.status == TransferStatus.cancelled) {
        session.status = TransferStatus.cancelled;
        TransferLogger.send('⚠️ 发送已中断取消 [$localSessionId]');
      } else {
        session.status = TransferStatus.failed;
        session.errorMessage = e.toString();
        TransferLogger.error('Send', '发送失败 [$localSessionId]: $e');
      }
      notifyListeners();
      return session;
    } finally {
      _activeSendingClients.remove(localSessionId);
      _activeSendingRemotes.remove(localSessionId);
      client.close();
    }
  }

  Stream<List<int>> _openFileOrMemoryStream(TransferFileItem fileItem) async* {
    if (fileItem.localPath != null) {
      yield* File(fileItem.localPath!).openRead();
    } else if (fileItem.memoryBytes != null) {
      final bytes = fileItem.memoryBytes!;
      const chunkSize = 64 * 1024;
      for (int offset = 0; offset < bytes.length; offset += chunkSize) {
        final end = min(offset + chunkSize, bytes.length);
        yield bytes.sublist(offset, end);
      }
    }
  }

  /// 反向拉取发送模式：适用于目标节点处于单向 NAT（如 Android 模拟器 10.0.2.16）或防火墙阻止入站连接的场景
  Future<TransferSession> _sendViaReversePull({
    required PeerDevice peer,
    required TransferSession session,
  }) async {
    final sessionId = session.sessionId;
    final token = _generateSecureToken();
    final ctx = _OutboundPullContext(
      session: session,
      peerId: peer.id,
      token: token,
    );
    _outboundPullSessions[sessionId] = ctx;

    try {
      TransferLogger.send(
        '等待对方节点 (${peer.alias}) 通过心跳通道拉取传输请求 [$sessionId]...',
      );

      final accepted = await ctx.decisionCompleter.future.timeout(
        const Duration(seconds: 65),
        onTimeout: () => false,
      );

      if (_cancelledSessionIds.contains(sessionId)) {
        session.status = TransferStatus.cancelled;
        notifyListeners();
        return session;
      }

      if (!accepted) {
        session.status = TransferStatus.rejected;
        TransferLogger.send('⚠️ 对方 (${peer.alias}) 拒绝或超时未响应反向拉取请求 [$sessionId]');
        notifyListeners();
        return session;
      }

      if (session.files.isEmpty) {
        session.status = TransferStatus.completed;
        TransferLogger.send('✅ 文本消息已通过反向通道送达 ${peer.alias} [$sessionId]');
        notifyListeners();
        return session;
      }

      session.status = TransferStatus.transferring;
      ctx.stopwatch.start();
      notifyListeners();

      TransferLogger.send(
        '对方 (${peer.alias}) 已确认接收 [$sessionId]，正在拉取文件流...',
      );

      await ctx.completionCompleter.future;

      if (_cancelledSessionIds.contains(sessionId)) {
        session.status = TransferStatus.cancelled;
        TransferLogger.send('⚠️ 反向拉取发送已取消 [$sessionId]');
        notifyListeners();
        return session;
      }

      final elapsedMs = max(1, ctx.stopwatch.elapsedMilliseconds);
      session.speedBytesPerSec = (session.totalBytes * 1000) / elapsedMs;
      session.transferredBytes = session.totalBytes;
      session.status = TransferStatus.completed;
      TransferLogger.send(
        '✅ 反向拉取发送全部完成 [$sessionId] -> ${peer.alias} '
        '| 共 ${session.files.length} 个文件 (${TransferSession.formatBytes(session.totalBytes)}) '
        '| 耗时: ${elapsedMs}ms | 速率: ${TransferSession.formatSpeed(session.speedBytesPerSec)}',
      );
      notifyListeners();
      return session;
    } catch (e) {
      if (_cancelledSessionIds.contains(sessionId)) {
        session.status = TransferStatus.cancelled;
      } else {
        session.status = TransferStatus.failed;
        session.errorMessage = e.toString();
        TransferLogger.error('Send', '反向拉取发送异常 [$sessionId]: $e');
      }
      notifyListeners();
      return session;
    } finally {
      _outboundPullSessions.remove(sessionId);
    }
  }

  Future<void> _handlePullDecision(HttpRequest request) async {
    final map = await _readJsonBodyLimited(request);
    final sessionId = map['sessionId'] as String? ?? '';
    final token = map['token'] as String? ?? '';
    final accepted = map['accepted'] as bool? ?? false;

    final ctx = _outboundPullSessions[sessionId];
    if (ctx != null && ctx.token == token) {
      if (!ctx.decisionCompleter.isCompleted) {
        ctx.decisionCompleter.complete(accepted);
      }
      if (!accepted && !ctx.completionCompleter.isCompleted) {
        ctx.completionCompleter.complete();
      }
    }

    request.response.headers.contentType = ContentType.json;
    request.response.write(jsonEncode({'ok': true}));
    await request.response.close();
  }

  Future<void> _handlePullFileStream(HttpRequest request) async {
    final sessionId = request.uri.queryParameters['sessionId'] ?? '';
    final fileId = request.uri.queryParameters['fileId'] ?? '';
    final token = request.headers.value('X-Upload-Token') ??
        request.uri.queryParameters['token'] ??
        '';

    final ctx = _outboundPullSessions[sessionId];
    if (ctx == null ||
        ctx.token != token ||
        _cancelledSessionIds.contains(sessionId)) {
      request.response.statusCode = HttpStatus.unauthorized;
      await request.response.close();
      return;
    }

    final matching = ctx.session.files.where((f) => f.id == fileId);
    if (matching.isEmpty) {
      request.response.statusCode = HttpStatus.notFound;
      await request.response.close();
      return;
    }
    final fileItem = matching.first;

    request.response.headers.contentType = ContentType.binary;
    request.response.contentLength = fileItem.size;
    request.response.bufferOutput = false;

    final rawStream = _openFileOrMemoryStream(fileItem);
    final trackedStream = rawStream.transform<List<int>>(
      StreamTransformer.fromHandlers(
        handleData: (chunk, sink) {
          if (_cancelledSessionIds.contains(sessionId)) {
            sink.addError(const SocketException('用户已取消发送'));
            return;
          }
          sink.add(chunk);
          ctx.session.transferredBytes += chunk.length;

          final elapsedMs = ctx.stopwatch.elapsedMilliseconds;
          if (elapsedMs - ctx.lastNotifyMs >= 100) {
            ctx.lastNotifyMs = elapsedMs;
            if (elapsedMs > 0) {
              ctx.session.speedBytesPerSec =
                  (ctx.session.transferredBytes * 1000) / elapsedMs;
            }
            notifyListeners();
          }
          if (elapsedMs - ctx.lastLogMs >= 500) {
            ctx.lastLogMs = elapsedMs;
            final pct = (ctx.session.progress * 100).toStringAsFixed(1);
            TransferLogger.send(
              '反向拉取发送进度 [$sessionId]: $pct% '
              '| 速率: ${TransferSession.formatSpeed(ctx.session.speedBytesPerSec)}',
            );
          }
        },
      ),
    );

    await request.response.addStream(trackedStream);
    await request.response.close();

    ctx.completedFilesCount++;
    if (ctx.completedFilesCount >= ctx.session.files.length &&
        !ctx.completionCompleter.isCompleted) {
      ctx.completionCompleter.complete();
    }
  }

  void _handleIncomingPullOffers(
    PeerDevice senderPeer,
    List<Map<String, dynamic>> offers,
  ) {
    for (final offer in offers) {
      final sessionId = offer['sessionId'] as String? ?? '';
      if (sessionId.isEmpty || !_processedPullOfferIds.add(sessionId)) {
        continue;
      }
      unawaited(_processSinglePullOffer(senderPeer, offer));
    }
  }

  Future<void> _processSinglePullOffer(
    PeerDevice senderPeer,
    Map<String, dynamic> offer,
  ) async {
    final sessionId = offer['sessionId'] as String? ?? '';
    final token = offer['token'] as String? ?? '';
    final rawFiles = (offer['files'] as List<dynamic>?) ?? [];
    final files = rawFiles
        .map((e) => TransferFileItem.fromJson(e as Map<String, dynamic>))
        .toList();
    final textMessage = offer['textMessage'] as String?;
    final totalBytes = files.fold<int>(0, (sum, f) => sum + f.size);

    final session = TransferSession(
      sessionId: sessionId,
      peerId: senderPeer.id,
      peerAlias: senderPeer.alias,
      peerAvatar: senderPeer.avatar,
      peerIp: senderPeer.ip,
      targetPeer: senderPeer,
      direction: TransferDirection.receiving,
      files: files,
      textMessage: textMessage,
      totalBytes: totalBytes,
      status: TransferStatus.waitingConfirmation,
    );

    _sessions.insert(0, session);
    notifyListeners();

    TransferLogger.recv(
      '收到反向拉取传输请求 <- 来自: ${senderPeer.alias} (${senderPeer.ip}:${senderPeer.transferPort}) '
      '| 会话: $sessionId | 文件数: ${files.length} '
      '| 总大小: ${TransferSession.formatBytes(totalBytes)}',
    );

    final isTrusted = _trustedPeerIds.contains(senderPeer.id);
    bool accepted = _autoAccept || isTrusted;
    if (!accepted && onIncomingTransferRequest != null) {
      final completer = Completer<bool>();
      _pendingConfirmations[sessionId] = completer;
      unawaited(
        onIncomingTransferRequest!(session).then((decision) {
          if (!completer.isCompleted) completer.complete(decision);
        }).catchError((Object _) {
          if (!completer.isCompleted) completer.complete(false);
        }),
      );
      accepted = await completer.future.timeout(
        const Duration(seconds: 60),
        onTimeout: () {
          onSessionCancelled?.call(sessionId);
          return false;
        },
      );
      _pendingConfirmations.remove(sessionId);
    } else if (!accepted && onIncomingTransferRequest == null) {
      accepted = true;
    }

    final client = HttpClient()
      ..connectionTimeout = const Duration(seconds: 8)
      ..autoUncompress = false;

    try {
      // 1. 回报确认结果
      final decisionUri = Uri.parse(
        'http://${senderPeer.ip}:${senderPeer.transferPort}/api/pull-decision',
      );
      final decReq = await client.postUrl(decisionUri);
      decReq.headers.contentType = ContentType.json;
      decReq.add(utf8.encode(jsonEncode({
        'sessionId': sessionId,
        'token': token,
        'accepted': accepted && !_cancelledSessionIds.contains(sessionId),
      })));
      final decResp = await decReq.close();
      await decResp.drain<void>();

      if (!accepted || _cancelledSessionIds.contains(sessionId)) {
        session.status = _cancelledSessionIds.contains(sessionId)
            ? TransferStatus.cancelled
            : TransferStatus.rejected;
        notifyListeners();
        return;
      }

      if (files.isEmpty) {
        session.status = TransferStatus.completed;
        TransferLogger.recv('✅ 文本消息接收完成 [$sessionId]');
        notifyListeners();
        return;
      }

      // 2. 逐个拉取文件流并原子落盘
      session.status = TransferStatus.transferring;
      notifyListeners();

      final saveDir = Directory(_saveDirectoryPath);
      if (!saveDir.existsSync()) {
        saveDir.createSync(recursive: true);
      }

      final stopwatch = Stopwatch()..start();
      int lastNotifyMs = 0;

      for (final fileItem in files) {
        if (_cancelledSessionIds.contains(sessionId)) {
          throw const SocketException('用户已取消接收');
        }

        final pullUri = Uri.parse(
          'http://${senderPeer.ip}:${senderPeer.transferPort}/api/pull-file'
          '?sessionId=${Uri.encodeComponent(sessionId)}'
          '&fileId=${Uri.encodeComponent(fileItem.id)}'
          '&token=${Uri.encodeComponent(token)}',
        );

        final pullReq = await client.getUrl(pullUri);
        pullReq.headers.set('X-Upload-Token', token);
        final pullResp = await pullReq.close();
        if (pullResp.statusCode != HttpStatus.ok) {
          await pullResp.drain<void>();
          throw HttpException('拉取文件失败 (${pullResp.statusCode})');
        }

        final finalFile = _resolveTargetFile(saveDir.path, fileItem);
        final partFile = File('${finalFile.path}.part');
        final sink = partFile.openWrite();
        int bytesInFile = 0;

        try {
          await for (final chunk in pullResp) {
            if (_cancelledSessionIds.contains(sessionId)) {
              throw const SocketException('用户已取消接收');
            }
            bytesInFile += chunk.length;
            sink.add(chunk);
            session.transferredBytes += chunk.length;

            final elapsedMs = stopwatch.elapsedMilliseconds;
            if (elapsedMs - lastNotifyMs >= 100) {
              lastNotifyMs = elapsedMs;
              if (elapsedMs > 0) {
                session.speedBytesPerSec =
                    (session.transferredBytes * 1000) / elapsedMs;
              }
              notifyListeners();
            }
          }
          await sink.flush();
          await sink.close();

          if (fileItem.size > 0 && bytesInFile != fileItem.size) {
            throw HttpException('拉取文件大小不匹配 ($bytesInFile != ${fileItem.size})');
          }

          final resolvedFinal = finalFile.existsSync()
              ? _resolveTargetFile(saveDir.path, fileItem)
              : finalFile;
          await partFile.rename(resolvedFinal.path);
          session.savedPaths.add(resolvedFinal.path);

          TransferLogger.recv(
            '反向拉取文件落盘完成 [$sessionId]: ${resolvedFinal.path} '
            '(${TransferSession.formatBytes(bytesInFile)})',
          );
        } catch (e) {
          await sink.close();
          try {
            if (partFile.existsSync()) partFile.deleteSync();
          } catch (_) {}
          rethrow;
        }
      }

      final elapsedMs = max(1, stopwatch.elapsedMilliseconds);
      session.speedBytesPerSec = (session.totalBytes * 1000) / elapsedMs;
      session.transferredBytes = session.totalBytes;
      session.status = TransferStatus.completed;
      TransferLogger.recv(
        '✅ 反向拉取会话全部接收完成 [$sessionId] | 共 ${session.savedPaths.length} 个文件',
      );
      notifyListeners();
    } catch (e) {
      if (_cancelledSessionIds.contains(sessionId)) {
        session.status = TransferStatus.cancelled;
      } else {
        session.status = TransferStatus.failed;
        session.errorMessage = e.toString();
        TransferLogger.error('Recv', '反向拉取接收失败 [$sessionId]: $e');
      }
      notifyListeners();
    } finally {
      client.close();
    }
  }

  /// 重试失败、被拒或已取消的发送任务
  Future<TransferSession?> retrySession(TransferSession oldSession) async {
    final peer = oldSession.targetPeer;
    if (peer == null) return null;
    _sessions.remove(oldSession);
    notifyListeners();
    return sendFilesToPeer(
      peer: peer,
      files: oldSession.files,
      textMessage: oldSession.textMessage,
    );
  }

  /// 内置轻量网页端：未安装 App 的手机或电脑浏览器访问 `http://<ip>:<port>` 也能直接发文件给本机
  Future<void> _handleWebPortal(HttpRequest request) async {
    final local = discoveryService.localDevice;
    const htmlEscape = HtmlEscape();
    final safeAlias = htmlEscape.convert(local.alias);
    final safeShortCode = htmlEscape.convert(local.shortCode);
    request.response.headers.contentType = ContentType.html;
    request.response.write('''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>邻传 - $safeAlias</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #09090b; color: #f4f4f5; margin: 0; padding: 24px; display: flex; justify-content: center; }
    .card { max-width: 440px; width: 100%; background: #121215; border: 1px solid #27272a; border-radius: 10px; padding: 24px; }
    .meta { font-family: monospace; color: #71717a; font-size: 12px; margin-bottom: 16px; }
    h2 { margin: 0 0 6px 0; font-size: 18px; font-weight: 600; }
    input[type="file"], textarea { width: 100%; box-sizing: border-box; margin: 10px 0; padding: 10px 12px; border-radius: 8px; border: 1px solid #27272a; background: #09090b; color: #f4f4f5; font-size: 13px; }
    button { width: 100%; background: #2563eb; color: white; border: none; padding: 11px; border-radius: 8px; font-size: 14px; font-weight: 500; cursor: pointer; margin-top: 8px; }
    button:hover { background: #1d4ed8; }
    #status { margin-top: 12px; font-size: 13px; color: #a1a1aa; font-family: monospace; }
  </style>
</head>
<body>
  <div class="card">
    <h2>$safeAlias</h2>
    <div class="meta">${local.ip}:${local.transferPort} · $safeShortCode</div>
    <input type="file" id="fileInput" multiple />
    <textarea id="textInput" rows="3" placeholder="输入文本内容..."></textarea>
    <button onclick="sendNow()">发送至 $safeAlias</button>
    <div id="status"></div>
  </div>
  <script>
    async function sendNow() {
      const fileEl = document.getElementById('fileInput');
      const textEl = document.getElementById('textInput');
      const files = fileEl.files;
      const text = textEl.value.trim();
      const status = document.getElementById('status');
      if (files.length === 0 && !text) { status.innerText = '请选择文件或输入文本'; return; }
      status.innerText = '等待对方确认...';
      const fileMeta = Array.from(files).map((f, i) => ({ id: String(i), name: f.name, size: f.size }));
      const prepResp = await fetch('/api/prepare-upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sender: { id: 'web-client', alias: 'Web 浏览器', avatar: 'web', deviceType: 'web', os: 'Browser', ip: '0.0.0.0', transferPort: 0 },
          files: fileMeta,
          textMessage: text || null
        })
      });
      if (!prepResp.ok) { status.innerText = '对方拒绝了请求'; return; }
      const { sessionId, token } = await prepResp.json();
      for (let i = 0; i < files.length; i++) {
        status.innerText = '正在传输 (' + (i + 1) + '/' + files.length + '): ' + files[i].name;
        const upResp = await fetch('/api/upload?sessionId=' + encodeURIComponent(sessionId) + '&fileId=' + i + '&fileName=' + encodeURIComponent(files[i].name) + '&token=' + encodeURIComponent(token), {
          method: 'POST',
          headers: { 'X-Upload-Token': token },
          body: files[i]
        });
        if (!upResp.ok) { status.innerText = '文件上传失败: ' + files[i].name; return; }
      }
      textEl.value = '';
      fileEl.value = '';
      status.innerText = '传输已完成';
    }
  </script>
</body>
</html>
''');
    await request.response.close();
  }

  void clearHistory() {
    _sessions.removeWhere((s) => !s.isActive);
    notifyListeners();
  }

  @override
  void dispose() {
    for (final c in _activeSendingClients.values) {
      c.close(force: true);
    }
    _activeSendingClients.clear();
    _server?.close(force: true);
    super.dispose();
  }
}
