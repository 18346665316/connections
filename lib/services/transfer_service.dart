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

class _UploadSegmentTask {
  final TransferFileItem fileItem;
  final int fileIndex;
  final int offset;
  final int length;
  final int segmentIndex;
  final int totalSegments;

  const _UploadSegmentTask({
    required this.fileItem,
    required this.fileIndex,
    required this.offset,
    required this.length,
    required this.segmentIndex,
    required this.totalSegments,
  });
}

class _ActiveFileReceiveContext {
  final TransferFileItem fileItem;
  final File finalFile;
  final File partFile;
  final int initialOffset;
  final Stopwatch fileStopwatch = Stopwatch()..start();
  RandomAccessFile? _raf;
  Future<void> _ioChain = Future<void>.value();
  final Map<int, ({int currentEnd, int targetEnd})> _segments = {};
  int contiguousBytes;
  int bytesSinceLastFlush = 0;
  bool isFinalized = false;
  bool isAborted = false;

  _ActiveFileReceiveContext({
    required this.fileItem,
    required this.finalFile,
    required this.partFile,
    required this.initialOffset,
  }) : contiguousBytes = initialOffset;

  Future<void> ensureOpen() {
    return _enqueueIo(() async {
      if (_raf != null || isAborted || isFinalized) return;
      _raf = await partFile.open(mode: FileMode.append);
      if (initialOffset == 0 && await _raf!.length() > 0) {
        await _raf!.truncate(0);
      }
    });
  }

  void registerSegment(int sliceStart, int sliceEnd) {
    _segments.putIfAbsent(
      sliceStart,
      () => (currentEnd: sliceStart, targetEnd: sliceEnd),
    );
  }

  Future<void> writeChunk({
    required int sliceStart,
    required int sliceEnd,
    required int writeOffset,
    required List<int> bytes,
    required int flushThresholdBytes,
  }) {
    return _enqueueIo(() async {
      if (isAborted || isFinalized) {
        throw const SocketException('文件接收上下文已关闭');
      }
      final raf = _raf ??= await partFile.open(mode: FileMode.append);
      await raf.setPosition(writeOffset);
      await raf.writeFrom(bytes);

      final newEnd = writeOffset + bytes.length;
      final existing = _segments[sliceStart];
      final prevEnd = existing?.currentEnd ?? sliceStart;
      _segments[sliceStart] = (
        currentEnd: max(prevEnd, newEnd),
        targetEnd: sliceEnd,
      );
      _advanceContiguousFrontier();

      bytesSinceLastFlush += bytes.length;
      if (bytesSinceLastFlush >= flushThresholdBytes) {
        bytesSinceLastFlush = 0;
        await raf.flush();
      }
    });
  }

  void _advanceContiguousFrontier() {
    final starts = _segments.keys.toList()..sort();
    for (final start in starts) {
      final seg = _segments[start]!;
      if (start <= contiguousBytes) {
        if (seg.currentEnd > contiguousBytes) {
          contiguousBytes = seg.currentEnd;
        }
        if (seg.currentEnd >= seg.targetEnd &&
            seg.targetEnd <= contiguousBytes) {
          _segments.remove(start);
        }
      } else {
        break;
      }
    }
  }

  Future<File?> tryFinalize(File Function() resolveFallbackFinal) {
    return _enqueueIo<File?>(() async {
      if (isFinalized || isAborted) return null;
      if (contiguousBytes < fileItem.size) return null;
      isFinalized = true;
      if (_raf != null) {
        await _raf!.flush();
        await _raf!.close();
        _raf = null;
      }
      final resolvedFinal =
          finalFile.existsSync() ? resolveFallbackFinal() : finalFile;
      await partFile.rename(resolvedFinal.path);
      return resolvedFinal;
    });
  }

  Future<void> abort({required bool deletePartFile}) {
    return _enqueueIo(() async {
      if (isFinalized || isAborted) return;
      isAborted = true;
      if (_raf != null) {
        try {
          if (!deletePartFile && contiguousBytes > 0) {
            await _raf!.truncate(contiguousBytes);
            await _raf!.flush();
          }
        } catch (_) {}
        try {
          await _raf!.close();
        } catch (_) {}
        _raf = null;
      }
      if (deletePartFile || contiguousBytes <= 0) {
        try {
          if (partFile.existsSync()) {
            partFile.deleteSync();
          }
        } catch (_) {}
      }
    });
  }

  Future<T> _enqueueIo<T>(Future<T> Function() action) {
    final completer = Completer<T>();
    _ioChain = _ioChain.then((_) async {
      try {
        final res = await action();
        completer.complete(res);
      } catch (e, st) {
        completer.completeError(e, st);
      }
    });
    return completer.future;
  }
}

class _OutboundPullContext {
  final TransferSession session;
  final String peerId;
  final bool isWebPeer;
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
    required this.isWebPeer,
    required this.token,
  });
}

class TransferService extends ChangeNotifier {
  static const int _maxControlBodyBytes = 1024 * 1024; // 1 MB 控制信令上限
  static const int _sendChunkCoalesceBytes = 512 * 1024; // 512 KB 发送聚合块
  static const int _diskWriteBufferBytes = 1024 * 1024; // 1 MB 磁盘顺序写聚合块
  static const int _flushBackpressureBytes = 8 * 1024 * 1024; // 8 MB 背压刷盘阈值
  static const int _parallelSegmentSizeBytes = 16 * 1024 * 1024; // 16 MB 大文件多路并发分片大小
  static const int _uiNotifyIntervalMs = 150; // 150ms UI 进度节流间隔

  final DiscoveryService discoveryService;
  HttpServer? _server;
  int _boundPort = DiscoveryService.defaultTransferPort;
  bool _autoAccept = false;
  late String _saveDirectoryPath;
  final Set<String> _trustedPeerIds = {};
  final Set<String> _ensuredDirPaths = {};
  final Set<String> _reservedTargetPaths = {};

  IncomingRequestResolver? onIncomingTransferRequest;
  ValueChanged<String>? onSessionCancelled;

  final List<TransferSession> _sessions = [];
  final Map<String, TransferSession> _activeReceivingSessions = {};
  final Map<String, Map<String, _ActiveFileReceiveContext>>
      _sessionFileContexts = {};
  final Map<String, int> _receivingSessionInitialBytes = {};
  final Map<String, Stopwatch> _receivingSessionStopwatches = {};
  final Map<String, int> _receivingSessionLastNotifyMs = {};
  final Map<String, int> _receivingSessionLastLogMs = {};
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

  /// 清理超过 24 小时未续传的陈旧 .part 临时文件（保留近期未传完的 .part 以支持断点续传）
  void _cleanStalePartFiles() {
    try {
      final dir = Directory(_saveDirectoryPath);
      if (!dir.existsSync()) return;
      final now = DateTime.now();
      for (final entity in dir.listSync(recursive: true, followLinks: false)) {
        if (entity is File && entity.path.endsWith('.part')) {
          try {
            final age = now.difference(entity.statSync().modified);
            if (age.inHours >= 24) {
              entity.deleteSync();
            }
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
      _ensuredDirPaths.clear();
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

    // 检查是否有挂起发往该节点的反向拉取任务（用于穿透 Android 模拟器 10.0.2.16 等单向 NAT 或 Web 浏览器投递）
    final pendingOffers = <Map<String, dynamic>>[];
    for (final ctx in _outboundPullSessions.values) {
      final shouldDeliver = ctx.peerId == peer.id &&
          !_cancelledSessionIds.contains(ctx.session.sessionId) &&
          (!ctx.offerDelivered || ctx.isWebPeer);
      if (shouldDeliver) {
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
    _receivingSessionInitialBytes.remove(sessionId);
    _receivingSessionLastNotifyMs.remove(sessionId);
    _receivingSessionLastLogMs.remove(sessionId);

    final fileContexts = _sessionFileContexts.remove(sessionId);
    if (fileContexts != null) {
      for (final fCtx in fileContexts.values) {
        _reservedTargetPaths.remove(fCtx.finalFile.path);
        unawaited(fCtx.abort(deletePartFile: true));
      }
    }

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

    _ensuredDirPaths.clear();
    _ensureDirectoryExists(_saveDirectoryPath);

    final fileContexts = <String, _ActiveFileReceiveContext>{};
    final resumeOffsets = <String, int>{};
    int initialSessionBytes = 0;

    for (final fileItem in files) {
      final resolved =
          _resolveTargetOrResumableFile(_saveDirectoryPath, fileItem);
      _reservedTargetPaths.add(resolved.finalFile.path);
      fileContexts[fileItem.id] = _ActiveFileReceiveContext(
        fileItem: fileItem,
        finalFile: resolved.finalFile,
        partFile: resolved.partFile,
        initialOffset: resolved.resumeOffset,
      );
      if (resolved.resumeOffset > 0) {
        resumeOffsets[fileItem.id] = resolved.resumeOffset;
        initialSessionBytes += resolved.resumeOffset;
      }
    }

    session.transferredBytes = initialSessionBytes;
    session.status = TransferStatus.transferring;
    _activeReceivingSessions[sessionId] = session;
    _sessionFileContexts[sessionId] = fileContexts;
    _receivingSessionInitialBytes[sessionId] = initialSessionBytes;
    _receivingSessionStopwatches[sessionId] = Stopwatch()..start();
    _receivingSessionLastNotifyMs[sessionId] = 0;
    _receivingSessionLastLogMs[sessionId] = 0;
    _sessionTokens[sessionId] = uploadToken;
    if (initialSessionBytes > 0) {
      TransferLogger.recv(
        '🔄 启用断点续传 [$sessionId] -> 已存在进度: ${TransferSession.formatBytes(initialSessionBytes)}',
      );
    }
    notifyListeners();

    request.response.headers.contentType = ContentType.json;
    request.response.write(jsonEncode({
      'accepted': true,
      'sessionId': sessionId,
      'token': uploadToken,
      'parallelChunkSize': _parallelSegmentSizeBytes,
      if (resumeOffsets.isNotEmpty) 'resumeOffsets': resumeOffsets,
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
    final sessionFilesMap = _sessionFileContexts[sessionId];

    if (session == null ||
        sessionFilesMap == null ||
        _cancelledSessionIds.contains(sessionId) ||
        expectedToken == null ||
        token != expectedToken) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.unauthorized;
      request.response.write(jsonEncode({'error': '未授权或已失效的传输会话'}));
      await request.response.close();
      return;
    }

    final fileCtx = sessionFilesMap[fileId];
    if (fileCtx == null) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.badRequest;
      request.response.write(jsonEncode({'error': '未声明的文件 ID'}));
      await request.response.close();
      return;
    }
    final fileItem = fileCtx.fileItem;

    final rawOffset = request.uri.queryParameters['offset'];
    final rawLength = request.uri.queryParameters['length'];
    final sliceStart =
        rawOffset != null ? (int.tryParse(rawOffset) ?? 0) : fileCtx.initialOffset;
    final sliceLength = rawLength != null
        ? (int.tryParse(rawLength) ?? max(0, fileItem.size - sliceStart))
        : max(0, fileItem.size - sliceStart);
    final sliceEnd = sliceStart + sliceLength;

    if (sliceStart < 0 ||
        sliceLength < 0 ||
        (fileItem.size > 0 && sliceEnd > fileItem.size)) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.badRequest;
      request.response.write(jsonEncode({'error': '分片偏移范围越界'}));
      await request.response.close();
      return;
    }

    await fileCtx.ensureOpen();
    fileCtx.registerSegment(sliceStart, sliceEnd);

    final writeBuffer = BytesBuilder(copy: false);
    final sessionStopwatch =
        _receivingSessionStopwatches[sessionId] ?? fileCtx.fileStopwatch;
    final initialSessionBytes = _receivingSessionInitialBytes[sessionId] ?? 0;
    int writeCursor = sliceStart;
    int bytesInSlice = 0;

    if (sliceStart == fileCtx.initialOffset) {
      TransferLogger.recv(
        '开始接收文件 [$sessionId] -> ${fileItem.relativePath ?? fileItem.name} '
        '(${TransferSession.formatBytes(fileItem.size)}'
        '${fileCtx.initialOffset > 0 ? ", 续传起点: ${TransferSession.formatBytes(fileCtx.initialOffset)}" : ""})',
      );
    }

    try {
      await for (final chunk in request) {
        if (_cancelledSessionIds.contains(sessionId)) {
          throw const SocketException('用户已取消接收');
        }
        bytesInSlice += chunk.length;
        if (fileItem.size > 0 && bytesInSlice > sliceLength) {
          throw const HttpException('接收字节数超出声明的分片大小');
        }
        writeBuffer.add(chunk);
        if (writeBuffer.length >= _diskWriteBufferBytes) {
          final bytesToWrite = writeBuffer.takeBytes();
          await fileCtx.writeChunk(
            sliceStart: sliceStart,
            sliceEnd: sliceEnd,
            writeOffset: writeCursor,
            bytes: bytesToWrite,
            flushThresholdBytes: _flushBackpressureBytes,
          );
          writeCursor += bytesToWrite.length;
        }
        session.transferredBytes += chunk.length;

        final elapsedMs = sessionStopwatch.elapsedMilliseconds;
        final lastNotifyMs = _receivingSessionLastNotifyMs[sessionId] ?? 0;
        if (elapsedMs - lastNotifyMs >= _uiNotifyIntervalMs) {
          _receivingSessionLastNotifyMs[sessionId] = elapsedMs;
          if (elapsedMs > 0) {
            final deltaBytes =
                max(0, session.transferredBytes - initialSessionBytes);
            session.speedBytesPerSec = (deltaBytes * 1000) / elapsedMs;
          }
          notifyListeners();
        }
        final lastLogMs = _receivingSessionLastLogMs[sessionId] ?? 0;
        if (elapsedMs - lastLogMs >= 500) {
          _receivingSessionLastLogMs[sessionId] = elapsedMs;
          final pct = (session.progress * 100).toStringAsFixed(1);
          TransferLogger.recv(
            '接收进度 [$sessionId] ${fileItem.name}: $pct% '
            '(${TransferSession.formatBytes(session.transferredBytes)} / ${TransferSession.formatBytes(session.totalBytes)}) '
            '| 速率: ${TransferSession.formatSpeed(session.speedBytesPerSec)}',
          );
        }
      }
      if (writeBuffer.isNotEmpty) {
        final bytesToWrite = writeBuffer.takeBytes();
        await fileCtx.writeChunk(
          sliceStart: sliceStart,
          sliceEnd: sliceEnd,
          writeOffset: writeCursor,
          bytes: bytesToWrite,
          flushThresholdBytes: _flushBackpressureBytes,
        );
        writeCursor += bytesToWrite.length;
      }

      if (fileItem.size > 0 && bytesInSlice != sliceLength) {
        throw HttpException(
          '分片完整性校验失败：预期 $sliceLength B，实际接收 $bytesInSlice B',
        );
      }

      File? finalizedFile;
      if (fileCtx.contiguousBytes >= fileItem.size) {
        _reservedTargetPaths.remove(fileCtx.finalFile.path);
        finalizedFile = await fileCtx.tryFinalize(
          () => _resolveTargetFile(_saveDirectoryPath, fileItem),
        );
      }

      if (finalizedFile != null) {
        final fileElapsedMs = max(1, fileCtx.fileStopwatch.elapsedMilliseconds);
        final sessionElapsedMs = max(1, sessionStopwatch.elapsedMilliseconds);
        final deltaBytes =
            max(0, session.transferredBytes - initialSessionBytes);
        session.speedBytesPerSec = (deltaBytes * 1000) / sessionElapsedMs;

        TransferLogger.recv(
          '文件落盘完成 [$sessionId]: ${finalizedFile.path} '
          '| 大小: ${TransferSession.formatBytes(fileItem.size)} '
          '| 耗时: ${fileElapsedMs}ms '
          '| 速率: ${TransferSession.formatSpeed(session.speedBytesPerSec)}',
        );

        session.savedPaths.add(finalizedFile.path);
        if (session.savedPaths.length >= session.files.length) {
          session.status = TransferStatus.completed;
          session.transferredBytes = session.totalBytes;
          _activeReceivingSessions.remove(sessionId);
          _sessionFileContexts.remove(sessionId);
          _receivingSessionInitialBytes.remove(sessionId);
          _receivingSessionStopwatches.remove(sessionId);
          _receivingSessionLastNotifyMs.remove(sessionId);
          _receivingSessionLastLogMs.remove(sessionId);
          _sessionTokens.remove(sessionId);
          TransferLogger.recv(
            '✅ 会话全部接收完成 [$sessionId] | 共 ${session.savedPaths.length} 个文件 '
            '(${TransferSession.formatBytes(session.totalBytes)})',
          );
        }
        notifyListeners();
      }

      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode({
        'ok': true,
        'savedPath': finalizedFile?.path ?? fileCtx.finalFile.path,
      }));
      await request.response.close();
    } catch (e) {
      final isCancelled = _cancelledSessionIds.contains(sessionId);
      if (!isCancelled && writeBuffer.isNotEmpty) {
        try {
          final remainingBytes = writeBuffer.takeBytes();
          await fileCtx.writeChunk(
            sliceStart: sliceStart,
            sliceEnd: sliceEnd,
            writeOffset: writeCursor,
            bytes: remainingBytes,
            flushThresholdBytes: _flushBackpressureBytes,
          );
          writeCursor += remainingBytes.length;
        } catch (_) {}
      }
      final allContexts = _sessionFileContexts.remove(sessionId);
      if (allContexts != null) {
        for (final fCtx in allContexts.values) {
          _reservedTargetPaths.remove(fCtx.finalFile.path);
          await fCtx.abort(deletePartFile: isCancelled);
        }
      } else {
        _reservedTargetPaths.remove(fileCtx.finalFile.path);
        await fileCtx.abort(deletePartFile: isCancelled);
      }

      if (isCancelled) {
        session.status = TransferStatus.cancelled;
        TransferLogger.recv('⚠️ 接收已取消 [$sessionId]: ${fileItem.name}');
      } else {
        session.status = TransferStatus.failed;
        session.errorMessage = e.toString();
        TransferLogger.error(
          'Recv',
          '接收中断 [$sessionId] (${fileItem.name}, 已保留连续断点 ${TransferSession.formatBytes(fileCtx.contiguousBytes)}): $e',
        );
      }
      _activeReceivingSessions.remove(sessionId);
      _receivingSessionInitialBytes.remove(sessionId);
      _receivingSessionStopwatches.remove(sessionId);
      _receivingSessionLastNotifyMs.remove(sessionId);
      _receivingSessionLastLogMs.remove(sessionId);
      _sessionTokens.remove(sessionId);
      notifyListeners();
      rethrow;
    }
  }

  void _ensureDirectoryExists(String dirPath) {
    if (_ensuredDirPaths.contains(dirPath)) return;
    final dir = Directory(dirPath);
    if (!dir.existsSync()) {
      dir.createSync(recursive: true);
    }
    _ensuredDirPaths.add(dirPath);
  }

  ({File finalFile, File partFile, int resumeOffset})
      _resolveTargetOrResumableFile(String baseDirPath, TransferFileItem item) {
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
        final targetSubDirPath =
            '$baseDirPath${Platform.pathSeparator}$subDirs';
        _ensureDirectoryExists(targetSubDirPath);
        return _resolveUniqueOrResumableFile(
          targetSubDirPath,
          rawSegments.last,
          item.size,
        );
      }
    }
    return _resolveUniqueOrResumableFile(baseDirPath, item.name, item.size);
  }

  ({File finalFile, File partFile, int resumeOffset})
      _resolveUniqueOrResumableFile(
    String dirPath,
    String rawFileName,
    int expectedSize,
  ) {
    final safeName =
        rawFileName.replaceAll(RegExp(r'[<>:"/\\|?*\x00-\x1F]'), '_');
    final dotIndex = safeName.lastIndexOf('.');
    final baseName =
        dotIndex > 0 ? safeName.substring(0, dotIndex) : safeName;
    final ext = dotIndex > 0 ? safeName.substring(dotIndex) : '';

    // 优先检查是否存在未完成的断点 .part 文件且未被其他活跃传输占用
    for (int counter = 0; counter < 50; counter++) {
      final candidate = counter == 0
          ? File('$dirPath${Platform.pathSeparator}$safeName')
          : File('$dirPath${Platform.pathSeparator}$baseName ($counter)$ext');
      if (_reservedTargetPaths.contains(candidate.path)) {
        continue;
      }
      final candidatePart = File('${candidate.path}.part');
      if (!candidate.existsSync() && candidatePart.existsSync()) {
        try {
          final existingLen = candidatePart.lengthSync();
          if (existingLen > 0 && existingLen < expectedSize) {
            return (
              finalFile: candidate,
              partFile: candidatePart,
              resumeOffset: existingLen,
            );
          } else if (existingLen == 0 || existingLen >= expectedSize) {
            // 大小为 0 或超过预期大小（可能同名不同文件），清理旧 .part 重新写入
            candidatePart.deleteSync();
            return (
              finalFile: candidate,
              partFile: candidatePart,
              resumeOffset: 0,
            );
          }
        } catch (_) {}
      }
    }

    final resolved = _resolveUniqueFile(dirPath, rawFileName);
    return (
      finalFile: resolved,
      partFile: File('${resolved.path}.part'),
      resumeOffset: 0,
    );
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
        final targetSubDirPath =
            '$baseDirPath${Platform.pathSeparator}$subDirs';
        _ensureDirectoryExists(targetSubDirPath);
        return _resolveUniqueFile(targetSubDirPath, rawSegments.last);
      }
    }
    return _resolveUniqueFile(baseDirPath, item.name);
  }

  File _resolveUniqueFile(String dirPath, String rawFileName) {
    final safeName =
        rawFileName.replaceAll(RegExp(r'[<>:"/\\|?*\x00-\x1F]'), '_');
    var candidate = File('$dirPath${Platform.pathSeparator}$safeName');
    if (!_reservedTargetPaths.contains(candidate.path) &&
        !candidate.existsSync() &&
        !File('${candidate.path}.part').existsSync()) {
      return candidate;
    }

    final dotIndex = safeName.lastIndexOf('.');
    final baseName =
        dotIndex > 0 ? safeName.substring(0, dotIndex) : safeName;
    final ext = dotIndex > 0 ? safeName.substring(dotIndex) : '';

    int counter = 1;
    while (_reservedTargetPaths.contains(candidate.path) ||
        candidate.existsSync() ||
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
      '发起传输请求 -> 目标: ${peer.alias} (${peer.endpointLabel}) '
      '| 会话: $localSessionId | 文件数: ${files.length} '
      '| 总大小: ${TransferSession.formatBytes(totalBytes)}'
      '${textMessage != null ? " | 附带文本: ${textMessage.length}字" : ""}',
    );

    // 若目标为 Web 浏览器节点，或处于 Android 模拟器 NAT 网段 (10.0.2.x)，自动走反向拉取通道
    final useReversePull = peer.deviceType == DeviceType.web ||
        peer.transferPort <= 0 ||
        (peer.ip.startsWith('10.0.2.') &&
            !discoveryService.localDevice.ip.startsWith('10.0.2.'));
    if (useReversePull) {
      TransferLogger.send(
        '目标 ${peer.alias} (${peer.endpointLabel}) 启用反向拉取 (Reverse-Pull) 通道...',
      );
      return _sendViaReversePull(
        peer: peer,
        session: session,
      );
    }

    final client = HttpClient()
      ..connectionTimeout = const Duration(seconds: 3)
      ..idleTimeout = const Duration(seconds: 30)
      ..maxConnectionsPerHost = 8
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
      final rawResumeOffsets =
          prepJson['resumeOffsets'] as Map<String, dynamic>? ?? const {};
      final parallelChunkSize =
          (prepJson['parallelChunkSize'] as num?)?.toInt() ??
              _parallelSegmentSizeBytes;

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

      // 构建多文件与单大文件多路分片任务队列（支持断点续传与 16MB 滑动窗口并发）
      final segmentTasks = <_UploadSegmentTask>[];
      int resumedTotal = 0;

      for (int i = 0; i < files.length; i++) {
        final fileItem = files[i];
        final rawResume =
            (rawResumeOffsets[fileItem.id] as num?)?.toInt() ?? 0;
        final resumeOffset = rawResume.clamp(0, fileItem.size);
        resumedTotal += resumeOffset;

        final remainingBytes = fileItem.size - resumeOffset;
        if (fileItem.size > 0 && remainingBytes == 0) {
          continue;
        }

        if (fileItem.size == 0 ||
            parallelChunkSize <= 0 ||
            remainingBytes <= parallelChunkSize) {
          segmentTasks.add(
            _UploadSegmentTask(
              fileItem: fileItem,
              fileIndex: i,
              offset: resumeOffset,
              length: remainingBytes,
              segmentIndex: 0,
              totalSegments: 1,
            ),
          );
        } else {
          final totalSegments =
              (remainingBytes + parallelChunkSize - 1) ~/ parallelChunkSize;
          int segIdx = 0;
          for (int offset = resumeOffset;
              offset < fileItem.size;
              offset += parallelChunkSize) {
            final segLen = min(parallelChunkSize, fileItem.size - offset);
            segmentTasks.add(
              _UploadSegmentTask(
                fileItem: fileItem,
                fileIndex: i,
                offset: offset,
                length: segLen,
                segmentIndex: segIdx++,
                totalSegments: totalSegments,
              ),
            );
          }
        }
      }

      session.transferredBytes = resumedTotal;
      session.status = TransferStatus.transferring;
      notifyListeners();

      final stopwatch = Stopwatch()..start();
      int lastNotifyMs = 0;
      int lastLogMs = 0;
      int bytesSentInCurrentRun = 0;

      // 多文件/大文件分片并发流水线传输（Keep-Alive 连接复用，最多 6 路并发）
      final concurrency =
          segmentTasks.isEmpty ? 1 : min(6, segmentTasks.length);
      TransferLogger.send(
        '对方已接受 [$localSessionId]，开始流式上传 '
        '(分片任务数: ${segmentTasks.length}, 并发通道数: $concurrency'
        '${resumedTotal > 0 ? ", 已跳过断点: ${TransferSession.formatBytes(resumedTotal)}" : ""})...',
      );
      int nextTaskIndex = 0;

      Future<void> uploadWorker() async {
        while (true) {
          if (_cancelledSessionIds.contains(localSessionId)) {
            return;
          }
          final taskIdx = nextTaskIndex++;
          if (taskIdx >= segmentTasks.length) return;
          final task = segmentTasks[taskIdx];
          final fileItem = task.fileItem;

          if (task.segmentIndex == 0) {
            TransferLogger.send(
              '上传文件 [${task.fileIndex + 1}/${files.length}]: '
              '${fileItem.relativePath ?? fileItem.name} '
              '(${TransferSession.formatBytes(fileItem.size)}'
              '${task.offset > 0 ? ", 续传起点: ${TransferSession.formatBytes(task.offset)}" : ""}'
              '${task.totalSegments > 1 ? ", 并发分片: ${task.totalSegments}片" : ""})',
            );
          }

          final uploadUri = Uri.parse(
            'http://${peer.ip}:${peer.transferPort}/api/upload'
            '?sessionId=${Uri.encodeComponent(remoteSessionId)}'
            '&fileId=${Uri.encodeComponent(fileItem.id)}'
            '&fileName=${Uri.encodeComponent(fileItem.name)}'
            '&offset=${task.offset}'
            '&length=${task.length}',
          );

          final uploadReq = await client.postUrl(uploadUri);
          uploadReq.headers.contentType = ContentType.binary;
          uploadReq.headers.set('X-Upload-Token', uploadToken);
          uploadReq.contentLength = task.length;

          final rawStream = _openFileOrMemoryStream(
            fileItem,
            start: task.offset,
            end: task.offset + task.length,
          );
          final coalesceBuffer = BytesBuilder(copy: false);
          final trackedStream = rawStream.transform<List<int>>(
            StreamTransformer.fromHandlers(
              handleData: (chunk, sink) {
                if (_cancelledSessionIds.contains(localSessionId)) {
                  sink.addError(const SocketException('用户已取消发送'));
                  return;
                }
                coalesceBuffer.add(chunk);
                if (coalesceBuffer.length >= _sendChunkCoalesceBytes) {
                  sink.add(coalesceBuffer.takeBytes());
                }
                bytesSentInCurrentRun += chunk.length;
                session.transferredBytes =
                    min(session.totalBytes, resumedTotal + bytesSentInCurrentRun);

                final elapsedMs = stopwatch.elapsedMilliseconds;
                if (elapsedMs - lastNotifyMs >= _uiNotifyIntervalMs) {
                  lastNotifyMs = elapsedMs;
                  if (elapsedMs > 0) {
                    session.speedBytesPerSec =
                        (bytesSentInCurrentRun * 1000) / elapsedMs;
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
              handleDone: (sink) {
                if (!_cancelledSessionIds.contains(localSessionId) &&
                    coalesceBuffer.isNotEmpty) {
                  sink.add(coalesceBuffer.takeBytes());
                } else {
                  coalesceBuffer.clear();
                }
                sink.close();
              },
            ),
          );

          await uploadReq.addStream(trackedStream);

          final uploadResp = await uploadReq.close();
          await uploadResp.drain<void>();
          if (uploadResp.statusCode != HttpStatus.ok) {
            throw Exception(
              '文件 ${fileItem.name} 分片传输失败 (${uploadResp.statusCode})',
            );
          }
        }
      }

      if (segmentTasks.isNotEmpty) {
        await Future.wait(List.generate(concurrency, (_) => uploadWorker()));
      }

      if (_cancelledSessionIds.contains(localSessionId)) {
        session.status = TransferStatus.cancelled;
        TransferLogger.send('⚠️ 发送已取消 [$localSessionId]');
        notifyListeners();
        return session;
      }

      final totalElapsedMs = max(1, stopwatch.elapsedMilliseconds);
      if (bytesSentInCurrentRun > 0) {
        session.speedBytesPerSec =
            (bytesSentInCurrentRun * 1000) / totalElapsedMs;
      }
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

  Stream<List<int>> _openFileOrMemoryStream(
    TransferFileItem fileItem, {
    int? start,
    int? end,
  }) {
    if (fileItem.localPath != null) {
      return File(fileItem.localPath!).openRead(start, end);
    }
    final bytes = fileItem.memoryBytes;
    if (bytes == null || bytes.isEmpty) {
      return const Stream<List<int>>.empty();
    }
    final startPos = (start ?? 0).clamp(0, bytes.length);
    final endPos = (end ?? bytes.length).clamp(startPos, bytes.length);
    if (startPos >= endPos) {
      return const Stream<List<int>>.empty();
    }
    final chunks = <List<int>>[];
    for (int offset = startPos;
        offset < endPos;
        offset += _sendChunkCoalesceBytes) {
      final chunkEnd = min(offset + _sendChunkCoalesceBytes, endPos);
      chunks.add(
        bytes is Uint8List
            ? Uint8List.sublistView(bytes, offset, chunkEnd)
            : bytes.sublist(offset, chunkEnd),
      );
    }
    return Stream<List<int>>.fromIterable(chunks);
  }

  /// 反向拉取发送模式：适用于目标节点处于单向 NAT（如 Android 模拟器 10.0.2.16）、Web 浏览器或防火墙阻止入站连接的场景
  Future<TransferSession> _sendViaReversePull({
    required PeerDevice peer,
    required TransferSession session,
  }) async {
    final sessionId = session.sessionId;
    final token = _generateSecureToken();
    final isWeb = peer.deviceType == DeviceType.web;
    final ctx = _OutboundPullContext(
      session: session,
      peerId: peer.id,
      isWebPeer: isWeb,
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

      if (session.files.isEmpty || isWeb) {
        ctx.stopwatch.start();
        session.transferredBytes = session.totalBytes;
        session.status = TransferStatus.completed;
        TransferLogger.send(
          isWeb
              ? '✅ 已投递至网页端收件箱 ${peer.alias} [$sessionId]'
              : '✅ 文本消息已通过反向通道送达 ${peer.alias} [$sessionId]',
        );
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
      if (isWeb) {
        // Web 端保留 30 分钟下载与刷新恢复凭证，允许手机浏览器用户随时点击下载或刷新页面
        Timer(const Duration(minutes: 30), () {
          _outboundPullSessions.remove(sessionId);
        });
      } else {
        _outboundPullSessions.remove(sessionId);
      }
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
    final rawOffset =
        int.tryParse(request.uri.queryParameters['offset'] ?? '') ?? 0;
    final startOffset = rawOffset.clamp(0, fileItem.size);
    final remainingLength = fileItem.size - startOffset;

    final encodedFileName = Uri.encodeComponent(fileItem.name);
    request.response.headers.contentType = ContentType.binary;
    request.response.headers.set(
      'Content-Disposition',
      'attachment; filename="$encodedFileName"; filename*=UTF-8\'\'$encodedFileName',
    );
    request.response.contentLength = remainingLength;

    if (!ctx.isWebPeer && startOffset > 0) {
      ctx.session.transferredBytes = min(
        ctx.session.totalBytes,
        ctx.session.transferredBytes + startOffset,
      );
    }

    final rawStream = _openFileOrMemoryStream(
      fileItem,
      start: startOffset,
      end: fileItem.size,
    );
    final coalesceBuffer = BytesBuilder(copy: false);
    final downloadStopwatch = Stopwatch()..start();
    int downloadBytes = 0;
    final trackedStream = rawStream.transform<List<int>>(
      StreamTransformer.fromHandlers(
        handleData: (chunk, sink) {
          if (_cancelledSessionIds.contains(sessionId)) {
            sink.addError(const SocketException('用户已取消发送'));
            return;
          }
          coalesceBuffer.add(chunk);
          if (coalesceBuffer.length >= _sendChunkCoalesceBytes) {
            sink.add(coalesceBuffer.takeBytes());
          }
          downloadBytes += chunk.length;
          if (!ctx.isWebPeer) {
            ctx.session.transferredBytes = min(
              ctx.session.totalBytes,
              ctx.session.transferredBytes + chunk.length,
            );
          }

          final elapsedMs = ctx.isWebPeer
              ? downloadStopwatch.elapsedMilliseconds
              : ctx.stopwatch.elapsedMilliseconds;
          if (elapsedMs - ctx.lastNotifyMs >= _uiNotifyIntervalMs) {
            ctx.lastNotifyMs = elapsedMs;
            if (elapsedMs > 0) {
              final measuredBytes =
                  ctx.isWebPeer ? downloadBytes : ctx.session.transferredBytes;
              ctx.session.speedBytesPerSec =
                  (measuredBytes * 1000) / elapsedMs;
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
        handleDone: (sink) {
          if (!_cancelledSessionIds.contains(sessionId) &&
              coalesceBuffer.isNotEmpty) {
            sink.add(coalesceBuffer.takeBytes());
          } else {
            coalesceBuffer.clear();
          }
          sink.close();
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
      _ensuredDirPaths.clear();
      session.status = TransferStatus.transferring;
      notifyListeners();

      _ensureDirectoryExists(_saveDirectoryPath);

      final stopwatch = Stopwatch()..start();
      int lastNotifyMs = 0;
      int bytesReceivedInRun = 0;

      for (final fileItem in files) {
        if (_cancelledSessionIds.contains(sessionId)) {
          throw const SocketException('用户已取消接收');
        }

        final targetInfo =
            _resolveTargetOrResumableFile(_saveDirectoryPath, fileItem);
        final finalFile = targetInfo.finalFile;
        final partFile = targetInfo.partFile;
        final resumeOffset = targetInfo.resumeOffset;

        if (resumeOffset > 0) {
          session.transferredBytes = min(
            session.totalBytes,
            session.transferredBytes + resumeOffset,
          );
        }

        final pullUri = Uri.parse(
          'http://${senderPeer.ip}:${senderPeer.transferPort}/api/pull-file'
          '?sessionId=${Uri.encodeComponent(sessionId)}'
          '&fileId=${Uri.encodeComponent(fileItem.id)}'
          '&token=${Uri.encodeComponent(token)}'
          '${resumeOffset > 0 ? "&offset=$resumeOffset" : ""}',
        );

        final pullReq = await client.getUrl(pullUri);
        pullReq.headers.set('X-Upload-Token', token);
        final pullResp = await pullReq.close();
        if (pullResp.statusCode != HttpStatus.ok) {
          await pullResp.drain<void>();
          throw HttpException('拉取文件失败 (${pullResp.statusCode})');
        }

        final sink = partFile.openWrite(
          mode: resumeOffset > 0 ? FileMode.append : FileMode.write,
        );
        final writeBuffer = BytesBuilder(copy: false);
        int bytesInFile = resumeOffset;
        int unflushedBytes = 0;

        try {
          await for (final chunk in pullResp) {
            if (_cancelledSessionIds.contains(sessionId)) {
              throw const SocketException('用户已取消接收');
            }
            bytesInFile += chunk.length;
            bytesReceivedInRun += chunk.length;
            writeBuffer.add(chunk);
            if (writeBuffer.length >= _diskWriteBufferBytes) {
              final bufferedLen = writeBuffer.length;
              sink.add(writeBuffer.takeBytes());
              unflushedBytes += bufferedLen;
              if (unflushedBytes >= _flushBackpressureBytes) {
                unflushedBytes = 0;
                await sink.flush();
              }
            }
            session.transferredBytes += chunk.length;

            final elapsedMs = stopwatch.elapsedMilliseconds;
            if (elapsedMs - lastNotifyMs >= _uiNotifyIntervalMs) {
              lastNotifyMs = elapsedMs;
              if (elapsedMs > 0) {
                session.speedBytesPerSec =
                    (bytesReceivedInRun * 1000) / elapsedMs;
              }
              notifyListeners();
            }
          }
          if (writeBuffer.isNotEmpty) {
            sink.add(writeBuffer.takeBytes());
          }
          await sink.flush();
          await sink.close();

          if (fileItem.size > 0 && bytesInFile != fileItem.size) {
            throw HttpException('拉取文件大小不匹配 ($bytesInFile != ${fileItem.size})');
          }

          final resolvedFinal = finalFile.existsSync()
              ? _resolveTargetFile(_saveDirectoryPath, fileItem)
              : finalFile;
          await partFile.rename(resolvedFinal.path);
          session.savedPaths.add(resolvedFinal.path);

          TransferLogger.recv(
            '反向拉取文件落盘完成 [$sessionId]: ${resolvedFinal.path} '
            '(${TransferSession.formatBytes(bytesInFile)})',
          );
        } catch (e) {
          if (writeBuffer.isNotEmpty) {
            try {
              sink.add(writeBuffer.takeBytes());
              await sink.flush();
            } catch (_) {}
          }
          await sink.close();
          if (_cancelledSessionIds.contains(sessionId)) {
            try {
              if (partFile.existsSync()) partFile.deleteSync();
            } catch (_) {}
          }
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

  /// 内置双向网页端：未安装 App 的手机或电脑浏览器扫码访问 `http://<ip>:<port>` 即可与本机双向收发文件与文本
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
    * { box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #09090b; color: #f4f4f5; margin: 0; padding: 20px 16px 40px; display: flex; justify-content: center; }
    .wrap { max-width: 480px; width: 100%; }
    .header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 18px; padding-bottom: 12px; border-bottom: 1px solid #27272a; }
    .title { font-size: 16px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
    .badge { font-family: monospace; font-size: 11.5px; color: #10b981; background: rgba(16, 185, 129, 0.12); padding: 3px 8px; border-radius: 5px; }
    .group-label { color: #71717a; font-size: 12px; font-weight: 500; margin: 18px 4px 8px; }
    .card { background: #121215; border: 1px solid #27272a; border-radius: 10px; padding: 16px; }
    .meta { font-family: monospace; color: #71717a; font-size: 12px; margin-bottom: 12px; }
    input[type="file"], textarea { width: 100%; margin: 8px 0; padding: 10px 12px; border-radius: 8px; border: 1px solid #27272a; background: #09090b; color: #f4f4f5; font-size: 13px; }
    button, .btn { display: inline-flex; align-items: center; justify-content: center; background: #2563eb; color: white; border: none; padding: 10px 14px; border-radius: 8px; font-size: 13.5px; font-weight: 500; cursor: pointer; text-decoration: none; }
    button.full { width: 100%; margin-top: 6px; }
    button:hover, .btn:hover { background: #1d4ed8; }
    .btn-outline { background: #18181c; border: 1px solid #27272a; color: #e4e4e7; padding: 6px 11px; font-size: 12px; border-radius: 6px; }
    .progress-wrap { display: none; width: 100%; height: 4px; background: #27272a; border-radius: 2px; overflow: hidden; margin-top: 10px; }
    .progress-bar { width: 0%; height: 100%; background: #3b82f6; transition: width 0.1s linear; }
    #status { margin-top: 8px; font-size: 12px; color: #a1a1aa; font-family: monospace; }
    .inbox-item { padding: 12px 0; border-top: 1px solid #222226; }
    .inbox-item:first-child { border-top: none; padding-top: 0; }
    .inbox-item:last-child { padding-bottom: 0; }
    .file-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-top: 8px; background: #09090b; border: 1px solid #27272a; border-radius: 7px; padding: 8px 10px; }
    .file-name { font-size: 13px; word-break: break-all; flex: 1; }
    .file-size { font-family: monospace; font-size: 11.5px; color: #71717a; margin-right: 6px; white-space: nowrap; }
    .text-box { margin-top: 8px; background: #09090b; border: 1px solid #27272a; border-radius: 7px; padding: 10px 12px; font-size: 13px; white-space: pre-wrap; word-break: break-all; }
    .empty-hint { color: #71717a; font-size: 13px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div class="title">邻传 · $safeAlias</div>
      <div class="badge" id="connBadge">已连接本机</div>
    </div>

    <div class="group-label">发送至 $safeAlias</div>
    <div class="card">
      <div class="meta">本机节点: ${local.ip}:${local.transferPort} ($safeShortCode)</div>
      <input type="file" id="fileInput" multiple />
      <textarea id="textInput" rows="3" placeholder="输入文本或粘贴链接发送到 $safeAlias..."></textarea>
      <button class="full" id="sendBtn" onclick="sendNow()">发送至 $safeAlias</button>
      <div class="progress-wrap" id="progressWrap"><div class="progress-bar" id="progressBar"></div></div>
      <div id="status"></div>
    </div>

    <div class="group-label">接收自 $safeAlias（在 App 中点击浏览器设备即可投递到此处）</div>
    <div class="card" id="inboxContainer">
      <div class="empty-hint" id="emptyHint">等待 $safeAlias 发送文件或文本...</div>
      <div id="inboxList"></div>
    </div>
  </div>

  <script>
    function detectBrowserOs() {
      const ua = navigator.userAgent || '';
      if (/iPhone/i.test(ua)) return 'iPhone';
      if (/iPad/i.test(ua)) return 'iPad';
      if (/Android/i.test(ua)) return 'Android';
      if (/Macintosh|Mac OS X/i.test(ua)) return 'macOS';
      if (/Windows/i.test(ua)) return 'Windows';
      if (/Linux/i.test(ua)) return 'Linux';
      return 'Web';
    }

    const osName = detectBrowserOs();
    let webPeerId = null;
    try {
      webPeerId = localStorage.getItem('connections_web_peer_id') || sessionStorage.getItem('connections_web_peer_id');
      if (!webPeerId) {
        webPeerId = 'web-' + Math.random().toString(36).substring(2, 10);
      }
      localStorage.setItem('connections_web_peer_id', webPeerId);
      sessionStorage.setItem('connections_web_peer_id', webPeerId);
    } catch (_) {
      webPeerId = 'web-' + Math.random().toString(36).substring(2, 10);
    }
    const webPeerProfile = {
      id: webPeerId,
      alias: osName + ' 浏览器',
      avatar: 'web',
      deviceType: 'web',
      os: osName + ' Web',
      ip: '0.0.0.0',
      transferPort: 0
    };

    const seenOfferIds = new Set();

    function formatBytes(bytes) {
      if (bytes < 1024) return bytes + ' B';
      if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
      if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
      return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
    }

    async function pollHeartbeat() {
      try {
        const resp = await fetch('/api/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(webPeerProfile)
        });
        if (!resp.ok) return;
        const data = await resp.json();
        const badge = document.getElementById('connBadge');
        badge.innerText = '在线 · ' + webPeerProfile.alias;
        if (Array.isArray(data.pullOffers)) {
          for (const offer of data.pullOffers) {
            if (!offer.sessionId || seenOfferIds.has(offer.sessionId)) continue;
            seenOfferIds.add(offer.sessionId);
            await handleIncomingOffer(offer);
          }
        }
      } catch (_) {
        document.getElementById('connBadge').innerText = '重连中...';
      }
    }

    async function handleIncomingOffer(offer) {
      // 立即通知本机：浏览器已收到推送会话
      await fetch('/api/pull-decision', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sessionId: offer.sessionId,
          token: offer.token,
          accepted: true
        })
      });

      const emptyHint = document.getElementById('emptyHint');
      if (emptyHint) emptyHint.style.display = 'none';

      const list = document.getElementById('inboxList');
      const item = document.createElement('div');
      item.className = 'inbox-item';

      const timeStr = new Date().toLocaleTimeString();
      let html = '<div class="meta">收到时间: ' + timeStr + '</div>';

      if (offer.textMessage) {
        const escapedText = offer.textMessage
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;');
        html += '<div class="text-box">' + escapedText + '</div>';
        html += '<div style="margin-top:8px;text-align:right;"><button class="btn-outline" data-copy="' + encodeURIComponent(offer.textMessage) + '" onclick="copyReceivedText(this)">复制文本</button></div>';
      }

      if (Array.isArray(offer.files) && offer.files.length > 0) {
        for (const f of offer.files) {
          const dlUrl = '/api/pull-file?sessionId=' + encodeURIComponent(offer.sessionId) +
            '&fileId=' + encodeURIComponent(f.id) +
            '&token=' + encodeURIComponent(offer.token);
          const safeName = (f.relativePath || f.name)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
          html += '<div class="file-row">' +
            '<span class="file-name">' + safeName + '</span>' +
            '<span class="file-size">' + formatBytes(f.size || 0) + '</span>' +
            '<a class="btn" style="padding:6px 12px;font-size:12.5px;" href="' + dlUrl + '" download>下载</a>' +
            '</div>';
        }
      }

      item.innerHTML = html;
      list.prepend(item);
    }

    function copyReceivedText(btn) {
      const text = decodeURIComponent(btn.getAttribute('data-copy') || '');
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
      const old = btn.innerText;
      btn.innerText = '已复制';
      setTimeout(() => { btn.innerText = old; }, 1500);
    }

    function uploadSingleFileWithProgress(url, token, file, onProgress) {
      return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', url, true);
        xhr.setRequestHeader('X-Upload-Token', token);
        xhr.upload.onprogress = (ev) => {
          if (ev.lengthComputable && onProgress) {
            onProgress(ev.loaded, ev.total);
          }
        };
        xhr.onload = () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            resolve();
          } else {
            reject(new Error('HTTP ' + xhr.status));
          }
        };
        xhr.onerror = () => reject(new Error('网络中断'));
        xhr.send(file);
      });
    }

    async function sendNow() {
      const fileEl = document.getElementById('fileInput');
      const textEl = document.getElementById('textInput');
      const sendBtn = document.getElementById('sendBtn');
      const pWrap = document.getElementById('progressWrap');
      const pBar = document.getElementById('progressBar');
      const files = fileEl.files;
      const text = textEl.value.trim();
      const status = document.getElementById('status');
      if (files.length === 0 && !text) { status.innerText = '请选择文件或输入文本'; return; }
      sendBtn.disabled = true;
      status.innerText = '等待对方确认...';
      try {
        const fileMeta = Array.from(files).map((f, i) => ({ id: String(i), name: f.name, size: f.size }));
        const prepResp = await fetch('/api/prepare-upload', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            sender: webPeerProfile,
            files: fileMeta,
            textMessage: text || null
          })
        });
        if (!prepResp.ok) { status.innerText = '对方拒绝了请求'; return; }
        const { sessionId, token } = await prepResp.json();
        const totalBytes = Array.from(files).reduce((acc, f) => acc + f.size, 0);
        let completedBytes = 0;
        const startTime = Date.now();
        if (files.length > 0) {
          pWrap.style.display = 'block';
          pBar.style.width = '0%';
        }
        for (let i = 0; i < files.length; i++) {
          const f = files[i];
          const url = '/api/upload?sessionId=' + encodeURIComponent(sessionId) +
            '&fileId=' + i +
            '&fileName=' + encodeURIComponent(f.name) +
            '&token=' + encodeURIComponent(token);
          await uploadSingleFileWithProgress(url, token, f, (loaded) => {
            const currentTotal = completedBytes + loaded;
            const pct = totalBytes > 0 ? Math.min(100, Math.round((currentTotal * 100) / totalBytes)) : 100;
            const elapsedSec = Math.max(0.1, (Date.now() - startTime) / 1000);
            const speed = formatBytes(Math.round(currentTotal / elapsedSec)) + '/s';
            pBar.style.width = pct + '%';
            status.innerText = '正在上传 (' + (i + 1) + '/' + files.length + ') ' + pct + '% · ' + speed;
          });
          completedBytes += f.size;
        }
        textEl.value = '';
        fileEl.value = '';
        pBar.style.width = '100%';
        status.innerText = '传输已完成';
        setTimeout(() => { pWrap.style.display = 'none'; }, 1200);
      } catch (err) {
        status.innerText = '发送失败: ' + (err.message || err);
        pWrap.style.display = 'none';
      } finally {
        sendBtn.disabled = false;
      }
    }

    pollHeartbeat();
    setInterval(pollHeartbeat, 2000);
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
