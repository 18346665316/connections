import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
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
  final Map<int, ({int currentEnd, int targetEnd})> _segments = {};
  final Map<int, int> _completedSegments = {};
  final Map<int, int> _maxSegmentReach = {};
  final Set<RandomAccessFile> _activeLocalHandles = {};
  int contiguousBytes;
  int completedContiguousBytes;
  bool startLogged = false;
  bool isFinalized = false;
  bool isAborted = false;

  _ActiveFileReceiveContext({
    required this.fileItem,
    required this.finalFile,
    required this.partFile,
    required this.initialOffset,
  })  : contiguousBytes = initialOffset,
        completedContiguousBytes = initialOffset;

  /// 在握手阶段预先创建或重置 .part 文件，使后续多路并发分片可各自持独立句柄无锁写入
  void preparePartFileSync() {
    if (isAborted || isFinalized) return;
    if (initialOffset == 0) {
      if (partFile.existsSync()) {
        partFile.writeAsBytesSync(const [], flush: false);
      } else {
        partFile.createSync(recursive: true);
      }
    } else if (!partFile.existsSync()) {
      partFile.createSync(recursive: true);
    }
  }

  void registerSegment(int sliceStart, int sliceEnd) {
    _segments.putIfAbsent(
      sliceStart,
      () => (currentEnd: sliceStart, targetEnd: sliceEnd),
    );
    _maxSegmentReach.putIfAbsent(sliceStart, () => sliceStart);
  }

  /// 更新分片进度并返回本次新增的不重复有效字节数（自动去重分片断线重传的前缀数据）
  int updateSegmentProgress(
    int sliceStart,
    int sliceEnd,
    int currentEnd,
  ) {
    if (isAborted || isFinalized) return 0;
    final clampedEnd = currentEnd.clamp(sliceStart, sliceEnd);
    final prevReach = _maxSegmentReach[sliceStart] ?? sliceStart;
    final netNewBytes = max(0, clampedEnd - prevReach);
    if (clampedEnd > prevReach) {
      _maxSegmentReach[sliceStart] = clampedEnd;
    }

    final existing = _segments[sliceStart];
    final prevEnd = existing?.currentEnd ?? sliceStart;
    _segments[sliceStart] = (
      currentEnd: max(prevEnd, clampedEnd),
      targetEnd: sliceEnd,
    );
    _advanceContiguousFrontier();
    return netNewBytes;
  }

  /// 标记分片完成并返回剩余尚未计入的不重复字节数
  int markSegmentCompleted(
    int sliceStart,
    int sliceEnd,
  ) {
    if (isAborted || isFinalized) return 0;
    final netNewBytes = updateSegmentProgress(sliceStart, sliceEnd, sliceEnd);
    final prevCompletedEnd = _completedSegments[sliceStart] ?? sliceStart;
    _completedSegments[sliceStart] = max(prevCompletedEnd, sliceEnd);
    _advanceCompletedFrontier();
    return netNewBytes;
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

  void _advanceCompletedFrontier() {
    final starts = _completedSegments.keys.toList()..sort();
    for (final start in starts) {
      final end = _completedSegments[start]!;
      if (start <= completedContiguousBytes) {
        if (end > completedContiguousBytes) {
          completedContiguousBytes = end;
        }
        if (end <= completedContiguousBytes) {
          _completedSegments.remove(start);
        }
      } else {
        break;
      }
    }
  }

  Future<File?> tryFinalize(File Function() resolveFallbackFinal) async {
    if (isFinalized || isAborted) return null;
    if (completedContiguousBytes < fileItem.size) return null;
    isFinalized = true;
    final resolvedFinal =
        finalFile.existsSync() ? resolveFallbackFinal() : finalFile;
    await partFile.rename(resolvedFinal.path);
    return resolvedFinal;
  }

  Future<void> abort({required bool deletePartFile}) async {
    if (isFinalized || isAborted) return;
    isAborted = true;
    final handles = _activeLocalHandles.toList();
    _activeLocalHandles.clear();
    for (final raf in handles) {
      try {
        await raf.close();
      } catch (_) {}
    }
    if (!deletePartFile && contiguousBytes > 0) {
      try {
        if (partFile.existsSync()) {
          final raf = await partFile.open(mode: FileMode.append);
          try {
            await raf.truncate(contiguousBytes);
          } finally {
            await raf.close();
          }
        }
      } catch (_) {}
    } else if (deletePartFile || contiguousBytes <= 0) {
      try {
        if (partFile.existsSync()) {
          partFile.deleteSync();
        }
      } catch (_) {}
    }
  }
}

/// 独立后台接收 Isolate 入口：监听高速数据端口，每个分片持独立文件句柄无锁落盘，与 UI 线程彻底隔离
Future<void> _dataServerIsolateEntry(Map<String, dynamic> initMsg) async {
  final readyPort = initMsg['readyPort'] as SendPort;
  final mainSendPort = initMsg['mainSendPort'] as SendPort;
  final requestedPort = initMsg['dataPort'] as int;
  const int diskWriteBufferBytes = 4 * 1024 * 1024;
  const int maxPendingWriteBytes = 16 * 1024 * 1024;

  final server = await HttpServer.bind(
    InternetAddress.anyIPv4,
    requestedPort,
    shared: true,
  );
  server.autoCompress = false;

  final authorizedSessions = <String, Map<String, dynamic>>{};
  final cancelledSessions = <String>{};
  final activeSessionRafs = <String, Set<RandomAccessFile>>{};

  final cmdPort = ReceivePort();
  cmdPort.listen((dynamic message) async {
    if (message is! Map) return;
    final cmd = message['cmd'] as String?;
    if (cmd == 'registerSession') {
      final sessionId = message['sessionId'] as String;
      cancelledSessions.remove(sessionId);
      authorizedSessions[sessionId] = Map<String, dynamic>.from(message);
      final ackPort = message['ackPort'] as SendPort?;
      ackPort?.send(true);
    } else if (cmd == 'cancelSession' || cmd == 'removeSession') {
      final sessionId = message['sessionId'] as String;
      if (cmd == 'cancelSession') {
        cancelledSessions.add(sessionId);
      }
      authorizedSessions.remove(sessionId);
      final rafs = activeSessionRafs.remove(sessionId);
      if (rafs != null) {
        for (final r in rafs.toList()) {
          try {
            await r.close();
          } catch (_) {}
        }
      }
    } else if (cmd == 'shutdown') {
      cmdPort.close();
      for (final rafSet in activeSessionRafs.values) {
        for (final r in rafSet.toList()) {
          try {
            await r.close();
          } catch (_) {}
        }
      }
      activeSessionRafs.clear();
      await server.close(force: true);
    }
  });

  server.listen((HttpRequest request) async {
    request.response.headers.set('Access-Control-Allow-Origin', '*');
    request.response.headers
        .set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    request.response.headers.set(
      'Access-Control-Allow-Headers',
      'Content-Type, X-Peer-Info, X-Upload-Token',
    );
    request.response.headers.set('Access-Control-Max-Age', '86400');

    if (request.method == 'OPTIONS') {
      request.response.statusCode = HttpStatus.noContent;
      await request.response.close();
      return;
    }

    if (request.uri.path != '/api/upload' || request.method != 'POST') {
      request.response.statusCode = HttpStatus.notFound;
      await request.response.close();
      return;
    }

    final sessionId = request.uri.queryParameters['sessionId'] ?? '';
    final fileId = request.uri.queryParameters['fileId'] ?? '';
    final token = request.headers.value('X-Upload-Token') ??
        request.uri.queryParameters['token'] ??
        '';

    final sessionMeta = authorizedSessions[sessionId];
    if (sessionMeta == null ||
        cancelledSessions.contains(sessionId) ||
        sessionMeta['token'] != token) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.unauthorized;
      request.response.write('{"error":"Unauthorized"}');
      await request.response.close();
      return;
    }

    final filesMap = sessionMeta['files'] as Map?;
    final rawFileMeta = filesMap?[fileId] as Map?;
    if (rawFileMeta == null) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.badRequest;
      request.response.write('{"error":"Invalid fileId"}');
      await request.response.close();
      return;
    }

    final partPath = rawFileMeta['partPath'] as String;
    final fileSize =
        (rawFileMeta['expectedSize'] as int?) ?? (rawFileMeta['size'] as int);
    final initialOffset = rawFileMeta['initialOffset'] as int;

    final rawOffset = request.uri.queryParameters['offset'];
    final rawLength = request.uri.queryParameters['length'];
    final sliceStart =
        rawOffset != null ? (int.tryParse(rawOffset) ?? 0) : initialOffset;
    final sliceLength = rawLength != null
        ? (int.tryParse(rawLength) ?? max(0, fileSize - sliceStart))
        : max(0, fileSize - sliceStart);
    final sliceEnd = sliceStart + sliceLength;

    if (sliceStart < 0 ||
        sliceLength < 0 ||
        (fileSize > 0 && sliceEnd > fileSize)) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.badRequest;
      request.response.write('{"error":"Slice out of range"}');
      await request.response.close();
      return;
    }

    mainSendPort.send({
      'type': 'sliceStart',
      'sessionId': sessionId,
      'fileId': fileId,
      'sliceStart': sliceStart,
      'sliceEnd': sliceEnd,
    });

    RandomAccessFile? sliceRaf;
    Future<void> sliceIoChain = Future<void>.value();
    int pendingWriteBytes = 0;
    Object? lastWriteError;
    final writeBuffer = BytesBuilder(copy: false);
    int writeCursor = sliceStart;
    int bytesInSlice = 0;
    int unreportedBytes = 0;
    final reportSw = Stopwatch()..start();
    int lastReportMs = 0;

    Future<void> enqueueSliceWrite(Uint8List bytesToWrite) {
      pendingWriteBytes += bytesToWrite.length;
      final completer = Completer<void>();
      sliceIoChain = sliceIoChain.then((_) async {
        try {
          if (cancelledSessions.contains(sessionId)) {
            throw const SocketException('用户已取消接收');
          }
          await sliceRaf!.writeFrom(bytesToWrite);
          completer.complete();
        } catch (e, st) {
          lastWriteError ??= e;
          completer.completeError(e, st);
        } finally {
          pendingWriteBytes = max(0, pendingWriteBytes - bytesToWrite.length);
        }
      });
      return completer.future;
    }

    try {
      request.response.bufferOutput = false;
      sliceRaf = await File(partPath).open(mode: FileMode.append);
      activeSessionRafs.putIfAbsent(sessionId, () => {}).add(sliceRaf);
      if (sliceLength > 0) {
        await sliceRaf.setPosition(sliceStart);
      }

      await for (final chunk in request) {
        if (cancelledSessions.contains(sessionId)) {
          throw const SocketException('用户已取消接收');
        }
        if (lastWriteError != null) {
          throw lastWriteError!;
        }
        bytesInSlice += chunk.length;
        unreportedBytes += chunk.length;
        if (fileSize > 0 && bytesInSlice > sliceLength) {
          throw const HttpException('接收字节数超出声明的分片大小');
        }
        writeBuffer.add(chunk);
        if (writeBuffer.length >= diskWriteBufferBytes) {
          final bytesToWrite = writeBuffer.takeBytes();
          writeCursor += bytesToWrite.length;
          final wf = enqueueSliceWrite(bytesToWrite);
          if (pendingWriteBytes >= maxPendingWriteBytes) {
            await wf;
          } else {
            unawaited(wf.catchError((Object _) {}));
          }
        }

        final nowMs = reportSw.elapsedMilliseconds;
        if (unreportedBytes >= 2 * 1024 * 1024 || nowMs - lastReportMs >= 80) {
          lastReportMs = nowMs;
          mainSendPort.send({
            'type': 'progress',
            'sessionId': sessionId,
            'fileId': fileId,
            'sliceStart': sliceStart,
            'sliceEnd': sliceEnd,
            'writeEnd': writeCursor,
            'receivedEnd': sliceStart + bytesInSlice,
            'deltaBytes': unreportedBytes,
          });
          unreportedBytes = 0;
        }
      }

      if (writeBuffer.isNotEmpty) {
        final bytesToWrite = writeBuffer.takeBytes();
        writeCursor += bytesToWrite.length;
        await enqueueSliceWrite(bytesToWrite);
      }
      await sliceIoChain;
      if (lastWriteError != null) {
        throw lastWriteError!;
      }

      activeSessionRafs[sessionId]?.remove(sliceRaf);
      await sliceRaf.close();
      sliceRaf = null;

      if (fileSize > 0 && bytesInSlice != sliceLength) {
        throw HttpException(
          '分片完整性校验失败：预期 $sliceLength B，实际接收 $bytesInSlice B',
        );
      }

      final replyPort = ReceivePort();
      mainSendPort.send({
        'type': 'sliceComplete',
        'sessionId': sessionId,
        'fileId': fileId,
        'sliceStart': sliceStart,
        'sliceEnd': sliceEnd,
        'remainingDeltaBytes': unreportedBytes,
        'replyPort': replyPort.sendPort,
      });
      unreportedBytes = 0;

      final reply = await replyPort.first.timeout(
        const Duration(seconds: 15),
        onTimeout: () => <String, dynamic>{'ok': true},
      );
      replyPort.close();

      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode(reply));
      await request.response.close();
    } catch (e) {
      if (sliceRaf != null &&
          !cancelledSessions.contains(sessionId) &&
          writeBuffer.isNotEmpty) {
        try {
          final remainingBytes = writeBuffer.takeBytes();
          writeCursor += remainingBytes.length;
          await enqueueSliceWrite(remainingBytes);
          await sliceIoChain;
        } catch (_) {}
      }
      if (sliceRaf != null) {
        activeSessionRafs[sessionId]?.remove(sliceRaf);
        try {
          await sliceIoChain;
        } catch (_) {}
        try {
          await sliceRaf.close();
        } catch (_) {}
        sliceRaf = null;
      }
      if (writeCursor > sliceStart) {
        mainSendPort.send({
          'type': 'progress',
          'sessionId': sessionId,
          'fileId': fileId,
          'sliceStart': sliceStart,
          'sliceEnd': sliceEnd,
          'writeEnd': writeCursor,
          'receivedEnd': writeCursor,
          'deltaBytes': unreportedBytes,
        });
      }
      mainSendPort.send({
        'type': 'sliceError',
        'sessionId': sessionId,
        'fileId': fileId,
        'sliceStart': sliceStart,
        'sliceEnd': sliceEnd,
        'cancelled': cancelledSessions.contains(sessionId),
        'error': e.toString(),
      });
      try {
        request.response.statusCode = HttpStatus.internalServerError;
        request.response.write(jsonEncode({'error': e.toString()}));
        await request.response.close();
      } catch (_) {}
    }
  });

  readyPort.send({
    'cmdPort': cmdPort.sendPort,
    'boundPort': server.port,
  });
}

/// 独立后台发送 Isolate 入口：持独立文件句柄与 Keep-Alive 连接发送 64MB 大分块，具备分块级指数退避自动重传
Future<void> _senderWorkerIsolateEntry(SendPort mainInitPort) async {
  final cmdPort = ReceivePort();
  final client = HttpClient()
    ..findProxy = null
    ..connectionTimeout = const Duration(seconds: 4)
    ..idleTimeout = const Duration(seconds: 30)
    ..maxConnectionsPerHost = 4
    ..autoUncompress = false;
  const int readChunkBytes = 2 * 1024 * 1024;
  const int maxAttempts = 4;

  cmdPort.listen((dynamic message) async {
    if (message is! Map) return;
    final cmd = message['cmd'] as String?;
    if (cmd == 'shutdown') {
      cmdPort.close();
      client.close(force: true);
      return;
    }
    if (cmd != 'uploadSlice') return;

    final replyPort = message['replyPort'] as SendPort;
    final url = message['url'] as String;
    final token = message['token'] as String;
    final localPath = message['localPath'] as String;
    final offset = message['offset'] as int;
    final length = message['length'] as int;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      int sentInAttempt = 0;

      Stream<List<int>> openSliceStream() async* {
        final raf = await File(localPath).open(mode: FileMode.read);
        try {
          if (offset > 0) {
            await raf.setPosition(offset);
          }
          int remaining = length;
          if (remaining <= 0) return;

          int currentReadSize = min(readChunkBytes, remaining);
          remaining -= currentReadSize;
          Future<Uint8List> pendingRead = raf.read(currentReadSize);

          while (true) {
            final chunk = await pendingRead;
            if (chunk.isEmpty) break;

            Future<Uint8List>? nextRead;
            if (remaining > 0 && chunk.length == currentReadSize) {
              final nextSize = min(readChunkBytes, remaining);
              remaining -= nextSize;
              currentReadSize = nextSize;
              nextRead = raf.read(nextSize);
            }

            sentInAttempt += chunk.length;
            replyPort.send({'type': 'progress', 'delta': chunk.length});
            yield chunk;

            if (nextRead == null) break;
            pendingRead = nextRead;
          }
        } finally {
          try {
            await raf.close();
          } catch (_) {}
        }
      }

      try {
        final uploadReq = await client.postUrl(Uri.parse(url));
        uploadReq.bufferOutput = false;
        uploadReq.headers.contentType = ContentType.binary;
        uploadReq.headers.set('X-Upload-Token', token);
        uploadReq.contentLength = length;

        await uploadReq.addStream(openSliceStream());
        final uploadResp = await uploadReq.close();
        await uploadResp.drain<void>();
        if (uploadResp.statusCode == HttpStatus.ok) {
          replyPort.send({'type': 'done', 'delta': 0});
          return;
        }
        if (uploadResp.statusCode == HttpStatus.unauthorized ||
            uploadResp.statusCode == HttpStatus.forbidden) {
          replyPort.send({
            'type': 'error',
            'error': '分片上传鉴权失败或已取消 (${uploadResp.statusCode})',
          });
          return;
        }
        throw HttpException('HTTP ${uploadResp.statusCode}');
      } catch (e) {
        if (sentInAttempt > 0) {
          // 回滚本次失败尝试已上报的临时进度，准备重试该分块
          replyPort.send({'type': 'progress', 'delta': -sentInAttempt});
        }
        if (attempt >= maxAttempts) {
          replyPort.send({'type': 'error', 'error': e.toString()});
          return;
        }
        await Future<void>.delayed(Duration(milliseconds: 400 * attempt));
      }
    }
  });

  mainInitPort.send(cmdPort.sendPort);
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
  static const int _fileReadChunkBytes = 2 * 1024 * 1024; // 2 MB 顺序磁盘读取与网络发送块
  static const int _diskWriteBufferBytes = 4 * 1024 * 1024; // 4 MB 接收端磁盘顺序写聚合块
  static const int _maxPendingWriteBytes = 16 * 1024 * 1024; // 16 MB 异步落盘队列背压上限
  static const int _parallelSegmentSizeBytes =
      64 * 1024 * 1024; // 64 MB 大分块交替流水线（小文件自动单流顺序直传，超大文件双流填满高 RTT 带宽时延积）
  static const int _maxParallelUploadStreams = 2; // 最多 2 路并发流，兼顾 BDP 吞吐与无线队列稳定性
  static const int _dataIsolateCount = 4; // 4 个独立后台数据接收 Isolate 池
  static const int _uiNotifyIntervalMs = 150; // 150ms UI 进度节流间隔

  final DiscoveryService discoveryService;
  HttpServer? _server;
  int _boundPort = DiscoveryService.defaultTransferPort;
  int _dataPort = 0;
  ReceivePort? _dataEngineReceivePort;
  final List<Isolate> _dataIsolates = [];
  final List<SendPort> _dataIsolateSendPorts = [];
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
  final Map<String, int> _receivingSessionWindowStartMs = {};
  final Map<String, int> _receivingSessionWindowStartBytes = {};
  final Map<String, String> _sessionTokens = {};
  final Map<String, Timer> _sessionRetryGraceTimers = {};
  final Map<String, Completer<bool>> _pendingConfirmations = {};
  final Map<String, HttpClient> _activeSendingClients = {};
  final Map<String, List<Isolate>> _activeSendingIsolates = {};
  final Map<String, Completer<void>> _sessionCancelCompleters = {};
  final Map<String, ({String ip, int port, String remoteSessionId})>
      _activeSendingRemotes = {};
  final Map<String, _OutboundPullContext> _outboundPullSessions = {};
  final Set<String> _processedPullOfferIds = {};
  final Set<String> _cancelledSessionIds = {};

  int get boundPort => _boundPort;
  int get dataPort => _dataPort > 0 ? _dataPort : _boundPort;
  bool get autoAccept => _autoAccept;
  String get saveDirectoryPath => _saveDirectoryPath;
  int get trustedPeerCount => _trustedPeerIds.length;
  List<TransferSession> get sessions => List.unmodifiable(_sessions);

  /// 是否存在正在进行的活跃数据传输（用于在传输期间自动静默 UDP 广播与后台轮询，把信道 100% 让给数据流）
  bool get hasActiveDataTransfer =>
      _activeReceivingSessions.isNotEmpty ||
      _activeSendingClients.isNotEmpty ||
      _sessions.any((s) => s.status == TransferStatus.transferring);

  TransferService({required this.discoveryService}) {
    discoveryService.onPullOffersReceived = _handleIncomingPullOffers;
    discoveryService.isTransferBusy = () => hasActiveDataTransfer;
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

    await _startDataIsolatePool();

    TransferLogger.recv(
      'P2P 传输服务已监听信令端口: $_boundPort | 高速数据流端口: $_dataPort '
      '(${_dataIsolateSendPorts.length} 核后台 Isolate 池) | 默认接收目录: $_saveDirectoryPath',
    );
    notifyListeners();
    return _boundPort;
  }

  /// 启动 4 核后台数据接收 Isolate 池（共享 _dataPort 端口，彻底隔离高频网络收包、磁盘 I/O 与 UI 渲染线程）
  Future<void> _startDataIsolatePool() async {
    try {
      int candidatePort = 0;
      for (int p = _boundPort + 100; p <= _boundPort + 120; p++) {
        try {
          final probe = await HttpServer.bind(
            InternetAddress.anyIPv4,
            p,
            shared: true,
          );
          candidatePort = probe.port;
          await probe.close(force: true);
          break;
        } catch (_) {
          continue;
        }
      }
      if (candidatePort == 0) {
        final probe = await HttpServer.bind(
          InternetAddress.anyIPv4,
          0,
          shared: true,
        );
        candidatePort = probe.port;
        await probe.close(force: true);
      }

      _dataEngineReceivePort?.close();
      final enginePort = ReceivePort();
      _dataEngineReceivePort = enginePort;
      enginePort.listen(_handleDataIsolateMessage);

      _dataIsolates.clear();
      _dataIsolateSendPorts.clear();

      for (int i = 0; i < _dataIsolateCount; i++) {
        final readyPort = ReceivePort();
        final isolate = await Isolate.spawn(
          _dataServerIsolateEntry,
          <String, dynamic>{
            'readyPort': readyPort.sendPort,
            'mainSendPort': enginePort.sendPort,
            'dataPort': candidatePort,
          },
          debugName: 'TransferDataIsolate-$i',
        );
        final readyMsg = await readyPort.first as Map<dynamic, dynamic>;
        readyPort.close();
        _dataIsolates.add(isolate);
        _dataIsolateSendPorts.add(readyMsg['cmdPort'] as SendPort);
      }
      _dataPort = candidatePort;
    } catch (e) {
      TransferLogger.error('Server', '后台数据 Isolate 池启动降级，回退至主端口: $e');
      _dataPort = _boundPort;
    }
  }

  void _handleDataIsolateMessage(dynamic raw) {
    if (raw is! Map) return;
    final type = raw['type'] as String? ?? '';
    final sessionId = raw['sessionId'] as String? ?? '';
    final fileId = raw['fileId'] as String? ?? '';

    if (_cancelledSessionIds.contains(sessionId)) return;
    final session = _activeReceivingSessions[sessionId];
    final sessionFilesMap = _sessionFileContexts[sessionId];
    if (session == null || sessionFilesMap == null) return;
    final fileCtx = sessionFilesMap[fileId];
    if (fileCtx == null) return;
    final fileItem = fileCtx.fileItem;

    if (type == 'sliceStart') {
      final sliceStart = raw['sliceStart'] as int? ?? 0;
      final sliceEnd = raw['sliceEnd'] as int? ?? 0;
      _sessionRetryGraceTimers.remove('$sessionId:$fileId:$sliceStart')?.cancel();
      fileCtx.registerSegment(sliceStart, sliceEnd);
      final sessionStopwatch =
          _receivingSessionStopwatches[sessionId] ?? fileCtx.fileStopwatch;
      if (!sessionStopwatch.isRunning) {
        sessionStopwatch.start();
      }
      if (!fileCtx.fileStopwatch.isRunning) {
        fileCtx.fileStopwatch.start();
      }
      if (sliceStart == fileCtx.initialOffset && !fileCtx.startLogged) {
        fileCtx.startLogged = true;
        TransferLogger.recv(
          '开始接收文件 [$sessionId] -> ${fileItem.relativePath ?? fileItem.name} '
          '(${TransferSession.formatBytes(fileItem.size)}'
          '${fileCtx.initialOffset > 0 ? ", 续传起点: ${TransferSession.formatBytes(fileCtx.initialOffset)}" : ""})',
        );
      }
    } else if (type == 'progress') {
      final sliceStart = raw['sliceStart'] as int? ?? 0;
      final sliceEnd = raw['sliceEnd'] as int? ?? 0;
      _sessionRetryGraceTimers.remove('$sessionId:$fileId:$sliceStart')?.cancel();
      final writeEnd = raw['writeEnd'] as int? ?? sliceStart;
      final receivedEnd = raw['receivedEnd'] as int? ?? writeEnd;

      final netNewBytes = fileCtx.updateSegmentProgress(
        sliceStart,
        sliceEnd,
        receivedEnd,
      );
      if (netNewBytes > 0) {
        session.transferredBytes = min(
          session.totalBytes,
          session.transferredBytes + netNewBytes,
        );
      }
      _updateReceivingProgressAndSpeed(sessionId, session, fileCtx);
    } else if (type == 'sliceComplete') {
      final sliceStart = raw['sliceStart'] as int? ?? 0;
      final sliceEnd = raw['sliceEnd'] as int? ?? 0;
      _sessionRetryGraceTimers.remove('$sessionId:$fileId:$sliceStart')?.cancel();
      final replyPort = raw['replyPort'] as SendPort?;

      final netNewBytes = fileCtx.markSegmentCompleted(sliceStart, sliceEnd);
      if (netNewBytes > 0) {
        session.transferredBytes = min(
          session.totalBytes,
          session.transferredBytes + netNewBytes,
        );
      }
      _updateReceivingProgressAndSpeed(sessionId, session, fileCtx);

      unawaited(
        _finalizeSliceAndRespond(
          sessionId: sessionId,
          session: session,
          fileCtx: fileCtx,
          replyPort: replyPort,
        ),
      );
    } else if (type == 'sliceError') {
      final sliceStart = raw['sliceStart'] as int? ?? 0;
      final sliceTimerKey = '$sessionId:$fileId:$sliceStart';
      final errorMsg = raw['error'] as String? ?? '接收分片异常';
      final isCancelled =
          (raw['cancelled'] as bool? ?? false) ||
          _cancelledSessionIds.contains(sessionId);
      if (isCancelled) {
        unawaited(_failReceivingSession(sessionId, session, fileCtx, errorMsg));
      } else {
        // 保留 8 秒宽限期等待发送端自动指数退避重传该分块，避免单次 Socket 闪断直接销毁整个会话
        TransferLogger.recv(
          '分片接收闪断 [$sessionId] (${fileItem.name} @ $sliceStart)，等待发送方自动重传: $errorMsg',
        );
        _sessionRetryGraceTimers[sliceTimerKey]?.cancel();
        _sessionRetryGraceTimers[sliceTimerKey] = Timer(
          const Duration(seconds: 8),
          () {
            _sessionRetryGraceTimers.remove(sliceTimerKey);
            unawaited(
              _failReceivingSession(sessionId, session, fileCtx, errorMsg),
            );
          },
        );
      }
    }
  }

  void _updateReceivingProgressAndSpeed(
    String sessionId,
    TransferSession session,
    _ActiveFileReceiveContext fileCtx,
  ) {
    discoveryService.touchPeer(session.peerId);
    final sessionStopwatch =
        _receivingSessionStopwatches[sessionId] ?? fileCtx.fileStopwatch;
    if (!sessionStopwatch.isRunning) {
      sessionStopwatch.start();
    }
    final initialSessionBytes = _receivingSessionInitialBytes[sessionId] ?? 0;
    final elapsedMs = sessionStopwatch.elapsedMilliseconds;
    final lastNotifyMs = _receivingSessionLastNotifyMs[sessionId] ?? 0;
    if (elapsedMs - lastNotifyMs >= _uiNotifyIntervalMs) {
      _receivingSessionLastNotifyMs[sessionId] = elapsedMs;
      final totalDelta = max(0, session.transferredBytes - initialSessionBytes);
      session.speedBytesPerSec = _computeSlidingSpeed(
        previousSpeed: session.speedBytesPerSec,
        totalDeltaBytes: totalDelta,
        elapsedMs: elapsedMs,
        windowStartMs: _receivingSessionWindowStartMs[sessionId] ?? 0,
        windowStartBytes: _receivingSessionWindowStartBytes[sessionId] ?? 0,
        onWindowReset: (newMs, newBytes) {
          _receivingSessionWindowStartMs[sessionId] = newMs;
          _receivingSessionWindowStartBytes[sessionId] = newBytes;
        },
      );
      notifyListeners();
    }

    final lastLogMs = _receivingSessionLastLogMs[sessionId] ?? 0;
    if (elapsedMs - lastLogMs >= 500) {
      _receivingSessionLastLogMs[sessionId] = elapsedMs;
      final pct = (session.progress * 100).toStringAsFixed(1);
      TransferLogger.recv(
        '接收进度 [$sessionId] ${fileCtx.fileItem.name}: $pct% '
        '(${TransferSession.formatBytes(session.transferredBytes)} / ${TransferSession.formatBytes(session.totalBytes)}) '
        '| 速率: ${TransferSession.formatSpeed(session.speedBytesPerSec)}',
      );
    }
  }

  Future<void> _finalizeSliceAndRespond({
    required String sessionId,
    required TransferSession session,
    required _ActiveFileReceiveContext fileCtx,
    SendPort? replyPort,
  }) async {
    try {
      final finalizedFile = await _checkAndFinalizeReceivedFile(
        sessionId: sessionId,
        session: session,
        fileCtx: fileCtx,
      );
      replyPort?.send({
        'ok': true,
        'savedPath': finalizedFile?.path ?? fileCtx.finalFile.path,
      });
    } catch (e) {
      replyPort?.send({'ok': false, 'error': e.toString()});
      await _failReceivingSession(
        sessionId,
        session,
        fileCtx,
        e.toString(),
      );
    }
  }

  Future<File?> _checkAndFinalizeReceivedFile({
    required String sessionId,
    required TransferSession session,
    required _ActiveFileReceiveContext fileCtx,
  }) async {
    final fileItem = fileCtx.fileItem;
    if (fileCtx.completedContiguousBytes < fileItem.size) {
      return null;
    }

    _reservedTargetPaths.remove(fileCtx.finalFile.path);
    final finalizedFile = await fileCtx.tryFinalize(
      () => _resolveTargetFile(_saveDirectoryPath, fileItem),
    );
    if (finalizedFile == null) {
      return null;
    }

    final sessionStopwatch =
        _receivingSessionStopwatches[sessionId] ?? fileCtx.fileStopwatch;
    final initialSessionBytes = _receivingSessionInitialBytes[sessionId] ?? 0;
    final fileElapsedMs = max(1, fileCtx.fileStopwatch.elapsedMilliseconds);
    final sessionElapsedMs = max(1, sessionStopwatch.elapsedMilliseconds);
    final deltaBytes = max(0, session.transferredBytes - initialSessionBytes);
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
      _cancelSessionRetryGraceTimers(sessionId);
      _activeReceivingSessions.remove(sessionId);
      _sessionFileContexts.remove(sessionId);
      _receivingSessionInitialBytes.remove(sessionId);
      _receivingSessionStopwatches.remove(sessionId);
      _receivingSessionLastNotifyMs.remove(sessionId);
      _receivingSessionLastLogMs.remove(sessionId);
      _receivingSessionWindowStartMs.remove(sessionId);
      _receivingSessionWindowStartBytes.remove(sessionId);
      _sessionTokens.remove(sessionId);
      _notifyDataIsolatesSessionRemoved(sessionId);
      TransferLogger.recv(
        '✅ 会话全部接收完成 [$sessionId] | 共 ${session.savedPaths.length} 个文件 '
        '(${TransferSession.formatBytes(session.totalBytes)})',
      );
    }
    notifyListeners();
    return finalizedFile;
  }

  void _cancelSessionRetryGraceTimers(String sessionId) {
    final prefix = '$sessionId:';
    final keysToRemove = _sessionRetryGraceTimers.keys
        .where((k) => k == sessionId || k.startsWith(prefix))
        .toList();
    for (final k in keysToRemove) {
      _sessionRetryGraceTimers.remove(k)?.cancel();
    }
  }

  Future<void> _failReceivingSession(
    String sessionId,
    TransferSession session,
    _ActiveFileReceiveContext fileCtx,
    Object error,
  ) async {
    _cancelSessionRetryGraceTimers(sessionId);
    if (!_activeReceivingSessions.containsKey(sessionId) &&
        session.status != TransferStatus.transferring) {
      return;
    }
    final isCancelled = _cancelledSessionIds.contains(sessionId);
    _notifyDataIsolatesSessionCancelled(sessionId);

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
      TransferLogger.recv('⚠️ 接收已取消 [$sessionId]: ${fileCtx.fileItem.name}');
    } else {
      session.status = TransferStatus.failed;
      session.errorMessage = error.toString();
      TransferLogger.error(
        'Recv',
        '接收中断 [$sessionId] (${fileCtx.fileItem.name}, 已保留连续断点 ${TransferSession.formatBytes(fileCtx.contiguousBytes)}): $error',
      );
    }
    _activeReceivingSessions.remove(sessionId);
    _receivingSessionInitialBytes.remove(sessionId);
    _receivingSessionStopwatches.remove(sessionId);
    _receivingSessionLastNotifyMs.remove(sessionId);
    _receivingSessionLastLogMs.remove(sessionId);
    _receivingSessionWindowStartMs.remove(sessionId);
    _receivingSessionWindowStartBytes.remove(sessionId);
    _sessionTokens.remove(sessionId);
    notifyListeners();
  }

  Future<void> _registerSessionOnDataIsolates({
    required String sessionId,
    required String token,
    required Map<String, _ActiveFileReceiveContext> fileContexts,
  }) async {
    if (_dataIsolateSendPorts.isEmpty) return;
    final filesPayload = <String, Map<String, dynamic>>{};
    for (final entry in fileContexts.entries) {
      filesPayload[entry.key] = {
        'partPath': entry.value.partFile.path,
        'expectedSize': entry.value.fileItem.size,
        'initialOffset': entry.value.initialOffset,
      };
    }
    final ackFutures = <Future<dynamic>>[];
    for (final sendPort in _dataIsolateSendPorts) {
      final ackPort = ReceivePort();
      sendPort.send({
        'cmd': 'registerSession',
        'sessionId': sessionId,
        'token': token,
        'files': filesPayload,
        'ackPort': ackPort.sendPort,
      });
      ackFutures.add(
        ackPort.first.whenComplete(ackPort.close),
      );
    }
    await Future.wait(ackFutures);
  }

  void _notifyDataIsolatesSessionCancelled(String sessionId) {
    for (final sendPort in _dataIsolateSendPorts) {
      sendPort.send({
        'cmd': 'cancelSession',
        'sessionId': sessionId,
      });
    }
  }

  void _notifyDataIsolatesSessionRemoved(String sessionId) {
    for (final sendPort in _dataIsolateSendPorts) {
      sendPort.send({
        'cmd': 'removeSession',
        'sessionId': sessionId,
      });
    }
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
        discoveryService.localAddresses.contains(remoteIp);
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
    _cancelSessionRetryGraceTimers(sessionId);
    _notifyDataIsolatesSessionCancelled(sessionId);
    final cancelComp = _sessionCancelCompleters.remove(sessionId);
    if (cancelComp != null && !cancelComp.isCompleted) {
      cancelComp.complete();
    }
    final sendingIsolates = _activeSendingIsolates.remove(sessionId);
    if (sendingIsolates != null) {
      for (final iso in sendingIsolates) {
        iso.kill(priority: Isolate.immediate);
      }
    }

    _sessionTokens.remove(sessionId);
    _receivingSessionStopwatches.remove(sessionId);
    _receivingSessionInitialBytes.remove(sessionId);
    _receivingSessionLastNotifyMs.remove(sessionId);
    _receivingSessionLastLogMs.remove(sessionId);
    _receivingSessionWindowStartMs.remove(sessionId);
    _receivingSessionWindowStartBytes.remove(sessionId);

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
        ..findProxy = null
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
      final fileCtx = _ActiveFileReceiveContext(
        fileItem: fileItem,
        finalFile: resolved.finalFile,
        partFile: resolved.partFile,
        initialOffset: resolved.resumeOffset,
      );
      fileCtx.preparePartFileSync();
      fileContexts[fileItem.id] = fileCtx;
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
    _receivingSessionStopwatches[sessionId] = Stopwatch();
    _receivingSessionLastNotifyMs[sessionId] = 0;
    _receivingSessionLastLogMs[sessionId] = 0;
    _receivingSessionWindowStartMs[sessionId] = 0;
    _receivingSessionWindowStartBytes[sessionId] = 0;
    _sessionTokens[sessionId] = uploadToken;

    await _registerSessionOnDataIsolates(
      sessionId: sessionId,
      token: uploadToken,
      fileContexts: fileContexts,
    );

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
      'dataPort': dataPort,
      'parallelChunkSize': _parallelSegmentSizeBytes,
      if (resumeOffsets.isNotEmpty) 'resumeOffsets': resumeOffsets,
    }));
    await request.response.close();
  }

  double _computeSlidingSpeed({
    required double previousSpeed,
    required int totalDeltaBytes,
    required int elapsedMs,
    required int windowStartMs,
    required int windowStartBytes,
    required void Function(int newStartMs, int newStartBytes) onWindowReset,
  }) {
    if (elapsedMs <= 0) return previousSpeed;
    final windowElapsedMs = elapsedMs - windowStartMs;
    if (windowElapsedMs >= 400) {
      final windowBytes = max(0, totalDeltaBytes - windowStartBytes);
      final instantSpeed = (windowBytes * 1000.0) / windowElapsedMs;
      onWindowReset(elapsedMs, totalDeltaBytes);
      return previousSpeed <= 0
          ? instantSpeed
          : (previousSpeed * 0.35 + instantSpeed * 0.65);
    }
    if (previousSpeed <= 0) {
      return (totalDeltaBytes * 1000.0) / elapsedMs;
    }
    return previousSpeed;
  }

  /// 主端口兼容/兜底文件上传处理器（每个分片使用独立的 RandomAccessFile 句柄，零 seek 竞争）
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

    final sliceTimerKey = '$sessionId:$fileId:$sliceStart';
    _sessionRetryGraceTimers.remove(sliceTimerKey)?.cancel();
    fileCtx.preparePartFileSync();
    fileCtx.registerSegment(sliceStart, sliceEnd);

    final sessionStopwatch =
        _receivingSessionStopwatches[sessionId] ?? fileCtx.fileStopwatch;
    if (!sessionStopwatch.isRunning) {
      sessionStopwatch.start();
    }
    if (!fileCtx.fileStopwatch.isRunning) {
      fileCtx.fileStopwatch.start();
    }

    if (sliceStart == fileCtx.initialOffset && !fileCtx.startLogged) {
      fileCtx.startLogged = true;
      TransferLogger.recv(
        '开始接收文件 [$sessionId] -> ${fileItem.relativePath ?? fileItem.name} '
        '(${TransferSession.formatBytes(fileItem.size)}'
        '${fileCtx.initialOffset > 0 ? ", 续传起点: ${TransferSession.formatBytes(fileCtx.initialOffset)}" : ""})',
      );
    }

    RandomAccessFile? sliceRaf;
    final writeBuffer = BytesBuilder(copy: false);
    int writeCursor = sliceStart;
    int bytesInSlice = 0;
    Future<void> writeChain = Future.value();
    int pendingWriteBytes = 0;
    Object? asyncWriteError;

    Future<void> enqueueSliceWrite(Uint8List bytes) {
      final writeEnd = writeCursor + bytes.length;
      writeCursor = writeEnd;
      pendingWriteBytes += bytes.length;
      final task = writeChain.then((_) async {
        if (asyncWriteError != null) return;
        await sliceRaf!.writeFrom(bytes);
      }).catchError((Object err) {
        asyncWriteError ??= err;
        throw err;
      }).whenComplete(() {
        pendingWriteBytes -= bytes.length;
      });
      writeChain = task.catchError((Object _) {});
      return task;
    }

    try {
      request.response.bufferOutput = false;
      sliceRaf = await fileCtx.partFile.open(mode: FileMode.append);
      fileCtx._activeLocalHandles.add(sliceRaf);
      if (sliceLength > 0) {
        await sliceRaf.setPosition(sliceStart);
      }

      await for (final chunk in request) {
        if (_cancelledSessionIds.contains(sessionId)) {
          throw const SocketException('用户已取消接收');
        }
        if (asyncWriteError != null) {
          throw asyncWriteError!;
        }
        bytesInSlice += chunk.length;
        if (fileItem.size > 0 && bytesInSlice > sliceLength) {
          throw const HttpException('接收字节数超出声明的分片大小');
        }
        writeBuffer.add(chunk);
        if (writeBuffer.length >= _diskWriteBufferBytes) {
          final bytesToWrite = writeBuffer.takeBytes();
          final writeFuture = enqueueSliceWrite(bytesToWrite);
          if (pendingWriteBytes >= _maxPendingWriteBytes) {
            await writeFuture;
          } else {
            unawaited(writeFuture.catchError((Object _) {}));
          }
        }
        final netNewBytes = fileCtx.updateSegmentProgress(
          sliceStart,
          sliceEnd,
          sliceStart + bytesInSlice,
        );
        if (netNewBytes > 0) {
          session.transferredBytes = min(
            session.totalBytes,
            session.transferredBytes + netNewBytes,
          );
        }
        _updateReceivingProgressAndSpeed(sessionId, session, fileCtx);
      }

      if (writeBuffer.isNotEmpty) {
        final bytesToWrite = writeBuffer.takeBytes();
        await enqueueSliceWrite(bytesToWrite);
      }
      await writeChain;
      if (asyncWriteError != null) {
        throw asyncWriteError!;
      }

      fileCtx._activeLocalHandles.remove(sliceRaf);
      await sliceRaf.close();
      sliceRaf = null;

      if (fileItem.size > 0 && bytesInSlice != sliceLength) {
        throw HttpException(
          '分片完整性校验失败：预期 $sliceLength B，实际接收 $bytesInSlice B',
        );
      }

      final remNewBytes = fileCtx.markSegmentCompleted(sliceStart, sliceEnd);
      if (remNewBytes > 0) {
        session.transferredBytes = min(
          session.totalBytes,
          session.transferredBytes + remNewBytes,
        );
      }
      final finalizedFile = await _checkAndFinalizeReceivedFile(
        sessionId: sessionId,
        session: session,
        fileCtx: fileCtx,
      );

      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode({
        'ok': true,
        'savedPath': finalizedFile?.path ?? fileCtx.finalFile.path,
      }));
      await request.response.close();
    } catch (e) {
      final isCancelled = _cancelledSessionIds.contains(sessionId);
      if (!isCancelled && sliceRaf != null && writeBuffer.isNotEmpty) {
        try {
          final remainingBytes = writeBuffer.takeBytes();
          await enqueueSliceWrite(remainingBytes);
          await writeChain;
        } catch (_) {}
      }
      if (sliceRaf != null) {
        fileCtx._activeLocalHandles.remove(sliceRaf);
        try {
          await sliceRaf.close();
        } catch (_) {}
        sliceRaf = null;
      }
      if (isCancelled) {
        await _failReceivingSession(sessionId, session, fileCtx, e);
      } else {
        TransferLogger.recv(
          '主端口分片接收闪断 [$sessionId] (${fileItem.name} @ $sliceStart)，等待发送方自动重传: $e',
        );
        _sessionRetryGraceTimers[sliceTimerKey]?.cancel();
        _sessionRetryGraceTimers[sliceTimerKey] = Timer(
          const Duration(seconds: 8),
          () {
            _sessionRetryGraceTimers.remove(sliceTimerKey);
            unawaited(_failReceivingSession(sessionId, session, fileCtx, e));
          },
        );
      }
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

  /// 多网卡路径优选：若目标设备同时暴露了雷雳网桥/有线直连地址与 Wi-Fi 地址，优先选择与本机同子网的高速直连 IP
  PeerDevice _resolveBestPeerEndpoint(PeerDevice peer) {
    if (peer.deviceType == DeviceType.web ||
        peer.transferPort <= 0 ||
        peer.ip == '127.0.0.1' ||
        peer.ip.startsWith('10.0.2.')) {
      return peer;
    }
    final candidates = peer.candidateIps;
    if (candidates.length <= 1) return peer;
    final localIps = discoveryService.localAddresses;

    // 1. 优先匹配两台 Mac 之间的雷雳网桥 / 网线直连链路本地网段 (169.254.0.0/16)
    final hasLocalLinkLocal = localIps.any((ip) => ip.startsWith('169.254.'));
    if (hasLocalLinkLocal) {
      for (final candidate in candidates) {
        if (candidate.startsWith('169.254.')) {
          return peer.copyWith(ip: candidate);
        }
      }
    }

    // 2. 精确匹配同 /24 子网前缀
    for (final localIp in localIps) {
      final dotIdx = localIp.lastIndexOf('.');
      if (dotIdx <= 0) continue;
      final prefix = localIp.substring(0, dotIdx + 1);
      for (final candidate in candidates) {
        if (candidate.startsWith(prefix)) {
          return peer.copyWith(ip: candidate);
        }
      }
    }

    // 3. 兼容匹配 /23 超网相邻子网前缀（如 192.168.20.x <-> 192.168.21.x）
    for (final localIp in localIps) {
      final parts = localIp.split('.');
      if (parts.length != 4) continue;
      final thirdOctet = int.tryParse(parts[2]);
      if (thirdOctet == null) continue;
      final companionPrefix = '${parts[0]}.${parts[1]}.${thirdOctet ^ 1}.';
      for (final candidate in candidates) {
        if (candidate.startsWith(companionPrefix)) {
          return peer.copyWith(ip: candidate);
        }
      }
    }
    return peer;
  }

  /// 向指定局域网设备发送文件列表（具备双流 64MB 大分块交替流水线、分块级自动退避重传、TCP 背压流控与令牌鉴权）
  Future<TransferSession> sendFilesToPeer({
    required PeerDevice peer,
    required List<TransferFileItem> files,
    String? textMessage,
  }) async {
    peer = _resolveBestPeerEndpoint(peer);
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
      ..findProxy = null
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
    final cancelCompleter = Completer<void>();
    _sessionCancelCompleters[localSessionId] = cancelCompleter;
    final spawnedSenderIsolates = <Isolate>[];
    _activeSendingIsolates[localSessionId] = spawnedSenderIsolates;

    try {
      HttpClientResponse? prepResp;
      Object? lastConnErr;
      for (final candidateIp in peer.candidateIps) {
        final prepUri = Uri.parse(
          'http://$candidateIp:${peer.transferPort}/api/prepare-upload',
        );
        try {
          final prepReq = await client.postUrl(prepUri);
          prepReq.headers.contentType = ContentType.json;
          prepReq.add(utf8.encode(jsonEncode({
            'clientSessionId': localSessionId,
            'sender': discoveryService.localDevice.toJson(),
            'files': files.map((f) => f.toJson()).toList(),
            'textMessage': textMessage,
          })));
          prepResp = await prepReq.close();
          if (candidateIp != peer.ip) {
            peer = peer.copyWith(ip: candidateIp);
          }
          break;
        } on Exception catch (connErr) {
          lastConnErr = connErr;
          if (_cancelledSessionIds.contains(localSessionId)) rethrow;
        }
      }
      if (prepResp == null) {
        if (_cancelledSessionIds.contains(localSessionId)) {
          throw lastConnErr ?? const SocketException('用户已取消发送');
        }
        TransferLogger.send(
          '直连 ${peer.ip}:${peer.transferPort} 不可达 ($lastConnErr)，自动切换为反向拉取 (Reverse-Pull) 通道...',
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
      final remoteDataPort =
          (prepJson['dataPort'] as num?)?.toInt() ?? peer.transferPort;
      final targetUploadPort =
          remoteDataPort > 0 ? remoteDataPort : peer.transferPort;
      final remoteChunkSize =
          (prepJson['parallelChunkSize'] as num?)?.toInt() ??
              _parallelSegmentSizeBytes;
      final segmentSize =
          remoteChunkSize > 0 ? remoteChunkSize : _parallelSegmentSizeBytes;
      final rawResumeOffsets =
          prepJson['resumeOffsets'] as Map<String, dynamic>? ?? const {};

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

      // 构建 64MB 大分块交替流水线队列（小文件直接单流顺序直传，超大文件切分为 64MB 分块由最多 2 个后台 Isolate 并行推流）
      final tasks = <_UploadSegmentTask>[];
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

        if (segmentSize > 0 &&
            fileItem.localPath != null &&
            remainingBytes > segmentSize) {
          final segCount = (remainingBytes + segmentSize - 1) ~/ segmentSize;
          for (int s = 0; s < segCount; s++) {
            final segOffset = resumeOffset + s * segmentSize;
            final segLen = min(segmentSize, fileItem.size - segOffset);
            tasks.add(
              _UploadSegmentTask(
                fileItem: fileItem,
                fileIndex: i,
                offset: segOffset,
                length: segLen,
                segmentIndex: s,
                totalSegments: segCount,
              ),
            );
          }
        } else {
          tasks.add(
            _UploadSegmentTask(
              fileItem: fileItem,
              fileIndex: i,
              offset: resumeOffset,
              length: remainingBytes,
              segmentIndex: 0,
              totalSegments: 1,
            ),
          );
        }
      }

      session.transferredBytes = resumedTotal;
      session.status = TransferStatus.transferring;
      notifyListeners();

      final stopwatch = Stopwatch()..start();
      int lastNotifyMs = 0;
      int lastLogMs = 0;
      int speedWindowStartMs = 0;
      int speedWindowStartBytes = 0;
      int bytesSentInCurrentRun = 0;

      void recordSentDelta(int delta) {
        if (delta == 0) return;
        bytesSentInCurrentRun = max(0, bytesSentInCurrentRun + delta);
        session.transferredBytes =
            min(session.totalBytes, resumedTotal + bytesSentInCurrentRun);
        if (delta < 0) {
          notifyListeners();
          return;
        }
        discoveryService.touchPeer(peer.id);

        final elapsedMs = stopwatch.elapsedMilliseconds;
        if (elapsedMs - lastNotifyMs >= _uiNotifyIntervalMs) {
          lastNotifyMs = elapsedMs;
          session.speedBytesPerSec = _computeSlidingSpeed(
            previousSpeed: session.speedBytesPerSec,
            totalDeltaBytes: bytesSentInCurrentRun,
            elapsedMs: elapsedMs,
            windowStartMs: speedWindowStartMs,
            windowStartBytes: speedWindowStartBytes,
            onWindowReset: (newMs, newBytes) {
              speedWindowStartMs = newMs;
              speedWindowStartBytes = newBytes;
            },
          );
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
      }

      final diskTasks = tasks
          .where((t) => t.fileItem.localPath != null)
          .toList(growable: false);
      final memoryTasks = tasks
          .where((t) => t.fileItem.localPath == null)
          .toList(growable: false);
      final workerCount = diskTasks.isEmpty
          ? 0
          : min(_maxParallelUploadStreams, diskTasks.length);

      TransferLogger.send(
        '对方已接受 [$localSessionId]，启动流式上传 -> 目标数据端口: $targetUploadPort '
        '(总分片: ${tasks.length}, 并发流数: $workerCount'
        '${resumedTotal > 0 ? ", 已跳过断点: ${TransferSession.formatBytes(resumedTotal)}" : ""})...',
      );

      // 1. 内存文件（如剪贴板或测试小文件）直接通过 Keep-Alive 连接发送（支持最多 3 次自动重试）
      for (final task in memoryTasks) {
        if (_cancelledSessionIds.contains(localSessionId)) break;
        final fileItem = task.fileItem;
        final uploadUri = Uri.parse(
          'http://${peer.ip}:$targetUploadPort/api/upload'
          '?sessionId=${Uri.encodeComponent(remoteSessionId)}'
          '&fileId=${Uri.encodeComponent(fileItem.id)}'
          '&fileName=${Uri.encodeComponent(fileItem.name)}'
          '&offset=${task.offset}'
          '&length=${task.length}'
          '&token=${Uri.encodeComponent(uploadToken)}',
        );
        const int maxMemAttempts = 3;
        for (int attempt = 1; attempt <= maxMemAttempts; attempt++) {
          int sentInAttempt = 0;
          try {
            final uploadReq = await client.postUrl(uploadUri);
            uploadReq.bufferOutput = false;
            uploadReq.headers.contentType = ContentType.binary;
            uploadReq.headers.set('X-Upload-Token', uploadToken);
            uploadReq.contentLength = task.length;

            final rawStream = _openFileOrMemoryStream(
              fileItem,
              start: task.offset,
              end: task.offset + task.length,
            );
            final trackedStream = rawStream.transform<List<int>>(
              StreamTransformer.fromHandlers(
                handleData: (chunk, sink) {
                  if (_cancelledSessionIds.contains(localSessionId)) {
                    sink.addError(const SocketException('用户已取消发送'));
                    return;
                  }
                  sink.add(chunk);
                  sentInAttempt += chunk.length;
                  recordSentDelta(chunk.length);
                },
              ),
            );
            await uploadReq.addStream(trackedStream);
            final uploadResp = await uploadReq.close();
            await uploadResp.drain<void>();
            if (uploadResp.statusCode == HttpStatus.ok) {
              break;
            }
            throw HttpException(
              '文件 ${fileItem.name} 传输失败 (${uploadResp.statusCode})',
            );
          } catch (err) {
            if (sentInAttempt > 0) {
              recordSentDelta(-sentInAttempt);
            }
            if (_cancelledSessionIds.contains(localSessionId) ||
                attempt >= maxMemAttempts) {
              rethrow;
            }
            await Future<void>.delayed(Duration(milliseconds: 300 * attempt));
          }
        }
      }

      // 2. 磁盘文件通过最多 2 个独立后台 Isolate 交替读取各自的 RandomAccessFile 句柄并并发推流
      if (diskTasks.isNotEmpty &&
          !_cancelledSessionIds.contains(localSessionId)) {
        final workerCmdPorts = <SendPort>[];
        for (int w = 0; w < workerCount; w++) {
          final initPort = ReceivePort();
          final iso = await Isolate.spawn(
            _senderWorkerIsolateEntry,
            initPort.sendPort,
            debugName: 'TransferSenderWorker-$w',
          );
          spawnedSenderIsolates.add(iso);
          final cmdPort = await initPort.first as SendPort;
          initPort.close();
          workerCmdPorts.add(cmdPort);
        }

        int nextTaskIdx = 0;
        Object? firstWorkerError;

        Future<void> runWorkerLoop(SendPort cmdPort) async {
          while (!_cancelledSessionIds.contains(localSessionId) &&
              firstWorkerError == null) {
            final idx = nextTaskIdx++;
            if (idx >= diskTasks.length) return;
            final task = diskTasks[idx];
            final fileItem = task.fileItem;

            if (task.segmentIndex == 0) {
              TransferLogger.send(
                '上传文件 [${task.fileIndex + 1}/${files.length}]: '
                '${fileItem.relativePath ?? fileItem.name} '
                '(${TransferSession.formatBytes(fileItem.size)}, 共 ${task.totalSegments} 个分片'
                '${task.offset > 0 ? ", 续传起点: ${TransferSession.formatBytes(task.offset)}" : ""})',
              );
            }

            final uploadUrl = 'http://${peer.ip}:$targetUploadPort/api/upload'
                '?sessionId=${Uri.encodeComponent(remoteSessionId)}'
                '&fileId=${Uri.encodeComponent(fileItem.id)}'
                '&fileName=${Uri.encodeComponent(fileItem.name)}'
                '&offset=${task.offset}'
                '&length=${task.length}'
                '&token=${Uri.encodeComponent(uploadToken)}';

            final replyPort = ReceivePort();
            final doneCompleter = Completer<void>();
            final sub = replyPort.listen((dynamic msg) {
              if (msg is! Map) return;
              final type = msg['type'] as String? ?? '';
              if (type == 'progress') {
                final delta = msg['delta'] as int? ?? 0;
                recordSentDelta(delta);
              } else if (type == 'done') {
                final delta = msg['delta'] as int? ?? 0;
                recordSentDelta(delta);
                if (!doneCompleter.isCompleted) {
                  doneCompleter.complete();
                }
              } else if (type == 'error') {
                final errMsg = msg['error'] as String? ?? '分片上传失败';
                if (!doneCompleter.isCompleted) {
                  doneCompleter.completeError(Exception(errMsg));
                }
              }
            });

            cmdPort.send({
              'cmd': 'uploadSlice',
              'replyPort': replyPort.sendPort,
              'url': uploadUrl,
              'token': uploadToken,
              'localPath': fileItem.localPath!,
              'offset': task.offset,
              'length': task.length,
            });

            try {
              await Future.any([
                doneCompleter.future,
                cancelCompleter.future,
              ]);
            } catch (err) {
              firstWorkerError ??= err;
              rethrow;
            } finally {
              await sub.cancel();
              replyPort.close();
            }
          }
        }

        try {
          await Future.wait(
            workerCmdPorts.map(runWorkerLoop),
          );
        } finally {
          for (final cmdPort in workerCmdPorts) {
            cmdPort.send({'cmd': 'shutdown'});
          }
        }
        if (firstWorkerError != null) {
          throw firstWorkerError!;
        }
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
      _sessionCancelCompleters.remove(localSessionId);
      final isolates = _activeSendingIsolates.remove(localSessionId);
      if (isolates != null) {
        for (final iso in isolates) {
          iso.kill(priority: Isolate.immediate);
        }
      }
      _activeSendingClients.remove(localSessionId);
      _activeSendingRemotes.remove(localSessionId);
      client.close();
    }
  }

  /// 双缓冲 2MB 顺序预读文件流（让磁盘下一块读取与当前块网络发送在后台并行重叠，消除 64KB openRead 瓶颈）
  Stream<List<int>> _openFileOrMemoryStream(
    TransferFileItem fileItem, {
    int? start,
    int? end,
  }) async* {
    if (fileItem.localPath != null) {
      final raf = await File(fileItem.localPath!).open(mode: FileMode.read);
      try {
        final fileLen = await raf.length();
        final startPos = (start ?? 0).clamp(0, fileLen);
        final endPos = (end ?? fileLen).clamp(startPos, fileLen);
        int remaining = endPos - startPos;
        if (remaining <= 0) return;

        if (startPos > 0) {
          await raf.setPosition(startPos);
        }

        int currentReadSize = min(_fileReadChunkBytes, remaining);
        remaining -= currentReadSize;
        Future<Uint8List> pendingRead = raf.read(currentReadSize);

        while (true) {
          final chunk = await pendingRead;
          if (chunk.isEmpty) break;

          Future<Uint8List>? nextRead;
          if (remaining > 0 && chunk.length == currentReadSize) {
            final nextSize = min(_fileReadChunkBytes, remaining);
            remaining -= nextSize;
            currentReadSize = nextSize;
            nextRead = raf.read(nextSize);
          }

          yield chunk;

          if (nextRead == null) break;
          pendingRead = nextRead;
        }
      } finally {
        try {
          await raf.close();
        } catch (_) {}
      }
      return;
    }

    final bytes = fileItem.memoryBytes;
    if (bytes == null || bytes.isEmpty) {
      return;
    }
    final startPos = (start ?? 0).clamp(0, bytes.length);
    final endPos = (end ?? bytes.length).clamp(startPos, bytes.length);
    for (int offset = startPos; offset < endPos; offset += _fileReadChunkBytes) {
      final chunkEnd = min(offset + _fileReadChunkBytes, endPos);
      yield bytes is Uint8List
          ? Uint8List.sublistView(bytes, offset, chunkEnd)
          : bytes.sublist(offset, chunkEnd);
    }
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
    int startOffset = rawOffset.clamp(0, fileItem.size);
    int endExclusive = fileItem.size;
    bool isRangeRequest = false;

    final rangeHeader = request.headers.value(HttpHeaders.rangeHeader);
    if (rangeHeader != null &&
        rangeHeader.startsWith('bytes=') &&
        fileItem.size > 0) {
      final rangeSpec = rangeHeader.substring('bytes='.length).split(',').first.trim();
      final parts = rangeSpec.split('-');
      if (parts.length == 2) {
        final parsedStart = int.tryParse(parts[0].trim());
        final parsedEnd = parts[1].trim().isEmpty
            ? (fileItem.size - 1)
            : int.tryParse(parts[1].trim());
        if (parsedStart != null &&
            parsedEnd != null &&
            parsedStart >= 0 &&
            parsedStart < fileItem.size &&
            parsedEnd >= parsedStart) {
          startOffset = parsedStart;
          endExclusive = min(fileItem.size, parsedEnd + 1);
          isRangeRequest = true;
        } else {
          request.response.statusCode = HttpStatus.requestedRangeNotSatisfiable;
          request.response.headers.set(
            HttpHeaders.contentRangeHeader,
            'bytes */${fileItem.size}',
          );
          await request.response.close();
          return;
        }
      }
    }

    final remainingLength = max(0, endExclusive - startOffset);

    final encodedFileName = Uri.encodeComponent(fileItem.name);
    request.response.bufferOutput = false;
    request.response.headers.contentType = ContentType.binary;
    request.response.headers.set(HttpHeaders.acceptRangesHeader, 'bytes');
    request.response.headers.set(
      'Content-Disposition',
      'attachment; filename="$encodedFileName"; filename*=UTF-8\'\'$encodedFileName',
    );
    if (isRangeRequest) {
      request.response.statusCode = HttpStatus.partialContent;
      request.response.headers.set(
        HttpHeaders.contentRangeHeader,
        'bytes $startOffset-${endExclusive - 1}/${fileItem.size}',
      );
    }
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
      end: endExclusive,
    );
    final downloadStopwatch = Stopwatch()..start();
    int downloadBytes = 0;
    final trackedStream = rawStream.transform<List<int>>(
      StreamTransformer.fromHandlers(
        handleData: (chunk, sink) {
          if (_cancelledSessionIds.contains(sessionId)) {
            sink.addError(const SocketException('用户已取消发送'));
            return;
          }
          sink.add(chunk);
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
      ),
    );

    await request.response.addStream(trackedStream);
    await request.response.close();

    if (endExclusive >= fileItem.size) {
      ctx.completedFilesCount++;
      if (ctx.completedFilesCount >= ctx.session.files.length &&
          !ctx.completionCompleter.isCompleted) {
        ctx.completionCompleter.complete();
      }
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
      ..findProxy = null
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

        try {
          await for (final chunk in pullResp) {
            if (_cancelledSessionIds.contains(sessionId)) {
              throw const SocketException('用户已取消接收');
            }
            bytesInFile += chunk.length;
            bytesReceivedInRun += chunk.length;
            writeBuffer.add(chunk);
            if (writeBuffer.length >= _diskWriteBufferBytes) {
              sink.add(writeBuffer.takeBytes());
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
    let isUploadingData = false;

    function formatBytes(bytes) {
      if (bytes < 1024) return bytes + ' B';
      if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
      if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
      return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
    }

    async function pollHeartbeat() {
      if (isUploadingData) return;
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

    function uploadSliceBlob(url, blob, onProgress) {
      return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', url, true);
        // 不附加自定义请求头，使 POST text/plain Blob 成为 CORS 简单请求（零 OPTIONS 预检 RTT 开销）
        xhr.upload.onprogress = (ev) => {
          if (ev.lengthComputable && onProgress) {
            onProgress(ev.loaded);
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
        xhr.send(blob);
      });
    }

    async function uploadSliceWithRetry(url, blob, onProgress) {
      const maxAttempts = 4;
      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          if (onProgress) onProgress(0);
          await uploadSliceBlob(url, blob, onProgress);
          return;
        } catch (err) {
          if (attempt >= maxAttempts) throw err;
          await new Promise(r => setTimeout(r, 400 * attempt));
        }
      }
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
        const prepData = await prepResp.json();
        const sessionId = prepData.sessionId;
        const token = prepData.token;
        const dataPort = prepData.dataPort || 0;
        const resumeOffsets = prepData.resumeOffsets || {};

        const totalBytes = Array.from(files).reduce((acc, f) => acc + f.size, 0);
        let completedFileBytes = 0;

        if (files.length > 0) {
          pWrap.style.display = 'block';
          pBar.style.width = '0%';
          isUploadingData = true;
        }

        const startTime = Date.now();
        let lastUiUpdateMs = 0;
        let windowStartMs = startTime;
        let windowStartBytes = 0;
        let smoothedSpeed = 0;

        function updateUiProgress(force, activeFileIdx, inFlightBytes, resumedTotal) {
          const now = Date.now();
          if (!force && now - lastUiUpdateMs < 150) return;
          lastUiUpdateMs = now;
          const currentTotal = Math.min(totalBytes, completedFileBytes + inFlightBytes);
          const pct = totalBytes > 0 ? Math.min(100, Math.round((currentTotal * 100) / totalBytes)) : 100;

          const winElapsed = now - windowStartMs;
          if (winElapsed >= 400) {
            const winBytes = Math.max(0, currentTotal - windowStartBytes);
            const instSpeed = (winBytes * 1000) / winElapsed;
            smoothedSpeed = smoothedSpeed <= 0 ? instSpeed : (smoothedSpeed * 0.35 + instSpeed * 0.65);
            windowStartMs = now;
            windowStartBytes = currentTotal;
          } else if (smoothedSpeed <= 0) {
            const totalElapsedSec = Math.max(0.1, (now - startTime) / 1000);
            smoothedSpeed = Math.max(0, currentTotal - resumedTotal) / totalElapsedSec;
          }

          const speedStr = formatBytes(Math.round(smoothedSpeed)) + '/s';
          pBar.style.width = pct + '%';
          status.innerText = '高速上传中 (' + (activeFileIdx + 1) + '/' + files.length + ') ' + pct + '% · ' + speedStr;
        }

        const dataOrigin = (dataPort > 0 && location.hostname)
          ? (location.protocol + '//' + location.hostname + ':' + dataPort)
          : '';
        let useFallbackOrigin = !dataOrigin;

        let resumedTotal = 0;
        for (let i = 0; i < files.length; i++) {
          const f = files[i];
          const fid = String(i);
          const startOff = Math.min(f.size, Math.max(0, Number(resumeOffsets[fid] || 0)));
          resumedTotal += startOff;
        }
        windowStartBytes = resumedTotal;

        const SLICE_BYTES = 64 * 1024 * 1024;
        for (let i = 0; i < files.length; i++) {
          const f = files[i];
          const fid = String(i);
          const startOff = Math.min(f.size, Math.max(0, Number(resumeOffsets[fid] || 0)));
          completedFileBytes += startOff;
          if (f.size > 0 && startOff >= f.size) {
            updateUiProgress(true, i, 0, resumedTotal);
            continue;
          }

          let cursor = startOff;
          do {
            const sliceEnd = Math.min(f.size, cursor + SLICE_BYTES);
            const sliceLen = Math.max(0, sliceEnd - cursor);
            const query = '/api/upload?sessionId=' + encodeURIComponent(sessionId) +
              '&fileId=' + encodeURIComponent(fid) +
              '&fileName=' + encodeURIComponent(f.name) +
              '&offset=' + cursor +
              '&length=' + sliceLen +
              '&token=' + encodeURIComponent(token);

            const payload = f.slice(cursor, sliceEnd, 'text/plain');
            const onProg = (loaded) => {
              updateUiProgress(false, i, loaded, resumedTotal);
            };
            try {
              const targetUrl = useFallbackOrigin ? query : (dataOrigin + query);
              await uploadSliceWithRetry(targetUrl, payload, onProg);
            } catch (err) {
              if (!useFallbackOrigin) {
                useFallbackOrigin = true;
                await uploadSliceWithRetry(query, payload, onProg);
              } else {
                throw err;
              }
            }
            completedFileBytes += sliceLen;
            cursor = sliceEnd;
            updateUiProgress(true, i, 0, resumedTotal);
          } while (cursor < f.size);
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
        isUploadingData = false;
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
    for (final timer in _sessionRetryGraceTimers.values) {
      timer.cancel();
    }
    _sessionRetryGraceTimers.clear();
    for (final c in _activeSendingClients.values) {
      c.close(force: true);
    }
    _activeSendingClients.clear();
    for (final comp in _sessionCancelCompleters.values) {
      if (!comp.isCompleted) comp.complete();
    }
    _sessionCancelCompleters.clear();
    for (final isolates in _activeSendingIsolates.values) {
      for (final iso in isolates) {
        iso.kill(priority: Isolate.immediate);
      }
    }
    _activeSendingIsolates.clear();
    for (final sendPort in _dataIsolateSendPorts) {
      sendPort.send({'cmd': 'shutdown'});
    }
    _dataIsolateSendPorts.clear();
    for (final iso in _dataIsolates) {
      iso.kill(priority: Isolate.immediate);
    }
    _dataIsolates.clear();
    _dataEngineReceivePort?.close();
    _server?.close(force: true);
    super.dispose();
  }
}
