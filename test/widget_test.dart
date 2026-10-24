import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:flutter_test/flutter_test.dart';
import 'package:connections/models/transfer_item.dart';
import 'package:connections/services/discovery_service.dart';
import 'package:connections/services/transfer_service.dart';

void main() {
  test('生产级 P2P 自动发现、Token 鉴权、.part 原子落盘与取消机制测试', () async {
    final discoveryA = DiscoveryService();
    final transferA = TransferService(discoveryService: discoveryA)
      ..setAutoAccept(true);

    final discoveryB = DiscoveryService();
    final transferB = TransferService(discoveryService: discoveryB)
      ..setAutoAccept(true);

    try {
      final portA = await transferA.startServer();
      final portB = await transferB.startServer();

      await discoveryA.initialize(boundTransferPort: portA);
      await discoveryB.initialize(boundTransferPort: portB);

      // 1. 节点 A 通过短码/IP 直连探测节点 B，验证双向发现
      final foundPeerB =
          await discoveryA.connectByCodeOrIp('127.0.0.1:$portB');
      expect(foundPeerB, isNotNull);
      expect(foundPeerB!.id, equals(discoveryB.localDevice.id));
      expect(
        discoveryB.peers.any((p) => p.id == discoveryA.localDevice.id),
        isTrue,
      );

      // 2. 验证未授权上传请求被 401 拦截
      final unauthClient = HttpClient();
      final unauthReq = await unauthClient.postUrl(
        Uri.parse(
          'http://127.0.0.1:$portB/api/upload?sessionId=fake&fileId=0&token=bad',
        ),
      );
      unauthReq.add([1, 2, 3]);
      final unauthResp = await unauthReq.close();
      await unauthResp.drain<void>();
      expect(unauthResp.statusCode, equals(HttpStatus.unauthorized));
      unauthClient.close();

      // 3. 验证带目录层级的文件流式互传与 .part 原子重命名
      final filePayload = utf8.encode('Production LAN P2P File Transfer Test');
      final session = await transferA.sendFilesToPeer(
        peer: foundPeerB.copyWith(ip: '127.0.0.1', transferPort: portB),
        files: [
          TransferFileItem(
            id: 'test-file-1',
            name: 'nested_note.txt',
            relativePath: 'TestFolder/sub_dir/nested_note.txt',
            size: filePayload.length,
            memoryBytes: filePayload,
          ),
        ],
        textMessage: '测试附言',
      );

      expect(session.status, equals(TransferStatus.completed));
      final receivedSession = transferB.sessions.first;
      expect(receivedSession.status, equals(TransferStatus.completed));
      final savedFile = File(receivedSession.savedPaths.first);
      expect(savedFile.existsSync(), isTrue);
      expect(File('${savedFile.path}.part').existsSync(), isFalse);
      expect(
        savedFile.readAsStringSync(),
        equals('Production LAN P2P File Transfer Test'),
      );

      // 清理测试目录
      final testFolderDir = savedFile.parent.parent;
      if (testFolderDir.existsSync() &&
          testFolderDir.path.endsWith('TestFolder')) {
        testFolderDir.deleteSync(recursive: true);
      }

      // 4. 验证等待确认阶段的远程取消通知机制
      transferB.setAutoAccept(false);
      String? cancelledIdOnB;
      final dialogCompleter = Completer<bool>();
      transferB.onIncomingTransferRequest = (_) => dialogCompleter.future;
      transferB.onSessionCancelled = (id) {
        cancelledIdOnB = id;
      };

      final sendFuture = transferA.sendFilesToPeer(
        peer: foundPeerB.copyWith(ip: '127.0.0.1', transferPort: portB),
        files: [
          TransferFileItem(
            id: 'cancel-file-1',
            name: 'cancel_me.txt',
            size: 10,
            memoryBytes: List<int>.filled(10, 65),
          ),
        ],
      );

      // 等待握手请求送达节点 B
      await Future<void>.delayed(const Duration(milliseconds: 120));
      final outgoingSession = transferA.sessions.first;
      expect(
        outgoingSession.status,
        equals(TransferStatus.waitingConfirmation),
      );

      // 发送方主动取消
      await transferA.cancelSession(outgoingSession.sessionId);
      final cancelledResult = await sendFuture;
      expect(cancelledResult.status, equals(TransferStatus.cancelled));
      expect(cancelledIdOnB, equals(outgoingSession.sessionId));
      expect(
        transferB.sessions.first.status,
        equals(TransferStatus.cancelled),
      );

      // 5. 验证针对 Android 模拟器 NAT 网段 (10.0.2.16) 的反向拉取 (Reverse-Pull) 穿透传输
      transferB.setAutoAccept(true);
      final natPeerB = foundPeerB.copyWith(
        ip: '10.0.2.16', // 模拟不可直连的 Android 模拟器内网 IP
        transferPort: portB,
      );
      final pullPayload = utf8.encode('Reverse Pull NAT Traversal Success!');
      final pullFuture = transferA.sendFilesToPeer(
        peer: natPeerB,
        files: [
          TransferFileItem(
            id: 'pull-file-1',
            name: 'emulator_pull_test.txt',
            size: pullPayload.length,
            memoryBytes: pullPayload,
          ),
        ],
      );

      // 模拟节点 B (Android 模拟器) 向宿主机 A 发起 /api/register 心跳并拉取任务
      await Future<void>.delayed(const Duration(milliseconds: 80));
      final regClient = HttpClient();
      final regReq = await regClient.postUrl(
        Uri.parse('http://127.0.0.1:$portA/api/register'),
      );
      regReq.headers.contentType = ContentType.json;
      regReq.add(utf8.encode(jsonEncode(discoveryB.localDevice.toJson())));
      final regResp = await regReq.close();
      final regBody = await utf8.decoder.bind(regResp).join();
      final regMap = jsonDecode(regBody) as Map<String, dynamic>;
      final offers = (regMap['pullOffers'] as List<dynamic>?)
              ?.whereType<Map<String, dynamic>>()
              .toList() ??
          [];
      expect(offers, isNotEmpty);
      discoveryB.onPullOffersReceived?.call(
        discoveryA.localDevice.copyWith(ip: '127.0.0.1', transferPort: portA),
        offers,
      );
      regClient.close();

      final pullResult = await pullFuture;
      expect(pullResult.status, equals(TransferStatus.completed));
      // 等待接收端完成磁盘 .part 文件 flush 与原子重命名
      for (int i = 0; i < 20; i++) {
        if (transferB.sessions.first.savedPaths.isNotEmpty) break;
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }
      final savedPullFile = File(transferB.sessions.first.savedPaths.first);
      expect(savedPullFile.existsSync(), isTrue);
      expect(
        savedPullFile.readAsStringSync(),
        equals('Reverse Pull NAT Traversal Success!'),
      );
      savedPullFile.deleteSync();
    } finally {
      transferA.dispose();
      transferB.dispose();
      discoveryA.dispose();
      discoveryB.dispose();
    }
  });
}
