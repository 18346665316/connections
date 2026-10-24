import 'dart:convert';
import 'dart:io';
import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:qr_flutter/qr_flutter.dart';
import '../models/peer_device.dart';
import '../models/transfer_item.dart';
import '../services/discovery_service.dart';
import '../services/transfer_service.dart';
import '../utils/file_opener.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> with WidgetsBindingObserver {
  late final DiscoveryService _discoveryService;
  late final TransferService _transferService;
  final Map<String, BuildContext> _openConfirmDialogContexts = {};

  bool _initialized = false;
  bool _isRefreshingPeers = false;

  bool get _isMobilePlatform => Platform.isAndroid || Platform.isIOS;

  Future<void> _handleRefreshPeers() async {
    if (_isRefreshingPeers) return;
    setState(() {
      _isRefreshingPeers = true;
    });
    _discoveryService.refreshDiscovery();
    await Future<void>.delayed(const Duration(milliseconds: 600));
    if (mounted) {
      setState(() {
        _isRefreshingPeers = false;
      });
    }
  }

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _discoveryService = DiscoveryService();
    _transferService = TransferService(discoveryService: _discoveryService);
    _transferService.onIncomingTransferRequest = _showIncomingConfirmDialog;
    _transferService.onSessionCancelled = _handleSessionCancelledRemotely;
    _bootServices();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed && _initialized) {
      _discoveryService.handleAppLifecycleResume();
    }
  }

  void _handleSessionCancelledRemotely(String sessionId) {
    final dialogCtx = _openConfirmDialogContexts.remove(sessionId);
    if (dialogCtx != null && dialogCtx.mounted) {
      Navigator.of(dialogCtx).pop(false);
    }
  }

  Future<void> _bootServices() async {
    final port = await _transferService.startServer();
    await _discoveryService.initialize(boundTransferPort: port);
    if (mounted) {
      setState(() {
        _initialized = true;
      });
    }
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _transferService.dispose();
    _discoveryService.dispose();
    super.dispose();
  }

  IconData _osIconByName(String os) {
    final lower = os.toLowerCase();
    if (lower.contains('mac')) return Icons.laptop_mac_rounded;
    if (lower.contains('ios')) return Icons.phone_iphone_rounded;
    if (lower.contains('android')) return Icons.phone_android_rounded;
    if (lower.contains('win')) return Icons.desktop_windows_rounded;
    if (lower.contains('linux')) return Icons.terminal_rounded;
    if (lower.contains('web') || lower.contains('browser')) {
      return Icons.language_rounded;
    }
    return Icons.devices_rounded;
  }

  IconData _platformIcon(PeerDevice device) => _osIconByName(device.os);

  Future<bool> _showIncomingConfirmDialog(TransferSession session) async {
    if (!mounted) return true;
    final result = await showDialog<bool>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) {
        _openConfirmDialogContexts[session.sessionId] = ctx;
        return AlertDialog(
          backgroundColor: const Color(0xFF121215),
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(10),
            side: const BorderSide(color: Color(0xFF27272A)),
          ),
          title: Row(
            children: [
              Container(
                width: 36,
                height: 36,
                decoration: BoxDecoration(
                  color: const Color(0xFF18181C),
                  borderRadius: BorderRadius.circular(8),
                  border: Border.all(color: const Color(0xFF27272A)),
                ),
                alignment: Alignment.center,
                child: Icon(
                  _osIconByName(session.peerAvatar),
                  size: 18,
                  color: const Color(0xFFF4F4F5),
                ),
              ),
              const SizedBox(width: 12),
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      '接收来自 ${session.peerAlias} 的传输',
                      style: const TextStyle(
                        color: Color(0xFFF4F4F5),
                        fontSize: 15.5,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                    const SizedBox(height: 2),
                    Text(
                      session.peerIp,
                      style: const TextStyle(
                        color: Color(0xFF71717A),
                        fontSize: 12,
                        fontFamily: 'monospace',
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ),
          content: ConstrainedBox(
            constraints: const BoxConstraints(maxWidth: 420),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                if (session.textMessage != null &&
                    session.textMessage!.trim().isNotEmpty) ...[
                  Container(
                    width: double.infinity,
                    padding: const EdgeInsets.all(12),
                    decoration: BoxDecoration(
                      color: const Color(0xFF09090B),
                      borderRadius: BorderRadius.circular(8),
                      border: Border.all(color: const Color(0xFF27272A)),
                    ),
                    child: Text(
                      session.textMessage!,
                      style: const TextStyle(
                        color: Color(0xFFE4E4E7),
                        fontSize: 13,
                      ),
                    ),
                  ),
                  const SizedBox(height: 12),
                ],
                if (session.files.isNotEmpty) ...[
                  Text(
                    '共 ${session.files.length} 项 · ${TransferSession.formatBytes(session.totalBytes)}',
                    style: const TextStyle(
                      color: Color(0xFFA1A1AA),
                      fontSize: 12.5,
                      fontWeight: FontWeight.w500,
                    ),
                  ),
                  const SizedBox(height: 8),
                  ConstrainedBox(
                    constraints: const BoxConstraints(maxHeight: 160),
                    child: SingleChildScrollView(
                      child: Column(
                        mainAxisSize: MainAxisSize.min,
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          for (final f in session.files.take(30))
                            Padding(
                              padding: const EdgeInsets.symmetric(vertical: 4),
                              child: Row(
                                children: [
                                  const Icon(
                                    Icons.insert_drive_file_outlined,
                                    size: 15,
                                    color: Color(0xFF71717A),
                                  ),
                                  const SizedBox(width: 8),
                                  Expanded(
                                    child: Text(
                                      f.relativePath ?? f.name,
                                      maxLines: 1,
                                      overflow: TextOverflow.ellipsis,
                                      style: const TextStyle(
                                        color: Color(0xFFF4F4F5),
                                        fontSize: 12.5,
                                      ),
                                    ),
                                  ),
                                  const SizedBox(width: 8),
                                  Text(
                                    TransferSession.formatBytes(f.size),
                                    style: const TextStyle(
                                      color: Color(0xFF71717A),
                                      fontSize: 11.5,
                                      fontFamily: 'monospace',
                                    ),
                                  ),
                                ],
                              ),
                            ),
                          if (session.files.length > 30)
                            Padding(
                              padding: const EdgeInsets.only(top: 6),
                              child: Text(
                                '... 另有 ${session.files.length - 30} 个文件',
                                style: const TextStyle(
                                  color: Color(0xFF71717A),
                                  fontSize: 12,
                                ),
                              ),
                            ),
                        ],
                      ),
                    ),
                  ),
                ],
              ],
            ),
          ),
          actionsOverflowButtonSpacing: 8,
          actions: [
            TextButton(
              onPressed: () => Navigator.of(ctx).pop(false),
              child: const Text(
                '拒绝',
                style: TextStyle(color: Color(0xFFEF4444)),
              ),
            ),
            OutlinedButton(
              style: OutlinedButton.styleFrom(
                foregroundColor: const Color(0xFFE4E4E7),
                side: const BorderSide(color: Color(0xFF27272A)),
                visualDensity: VisualDensity.compact,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(6),
                ),
              ),
              onPressed: () {
                _transferService.toggleTrustPeer(session.peerId, trust: true);
                if (mounted) setState(() {});
                Navigator.of(ctx).pop(true);
              },
              child: const Text('信任并接收', style: TextStyle(fontSize: 12.5)),
            ),
            FilledButton(
              style: FilledButton.styleFrom(
                backgroundColor: const Color(0xFF2563EB),
                foregroundColor: Colors.white,
                visualDensity: VisualDensity.compact,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(6),
                ),
              ),
              onPressed: () => Navigator.of(ctx).pop(true),
              child: const Text('接收', style: TextStyle(fontSize: 12.5)),
            ),
          ],
        );
      },
    );
    _openConfirmDialogContexts.remove(session.sessionId);
    return result ?? false;
  }

  Future<void> _sendClipboardInstantly(PeerDevice peer) async {
    final data = await Clipboard.getData(Clipboard.kTextPlain);
    final text = data?.text?.trim() ?? '';
    if (text.isEmpty) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('当前剪贴板为空'),
          behavior: SnackBarBehavior.floating,
        ),
      );
      return;
    }
    final session = await _transferService.sendFilesToPeer(
      peer: peer,
      files: const [],
      textMessage: text,
    );
    if (!mounted) return;
    if (session.status == TransferStatus.completed) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('剪贴板文本已发送至 ${peer.alias}'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    }
  }

  Future<void> _showMorePeerActions(PeerDevice peer) async {
    final isTrusted = _transferService.isPeerTrusted(peer.id);
    showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      backgroundColor: const Color(0xFF121215),
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(14)),
        side: BorderSide(color: Color(0xFF27272A)),
      ),
      builder: (ctx) {
        return SafeArea(
          child: SingleChildScrollView(
            padding: const EdgeInsets.fromLTRB(16, 16, 16, 20),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Padding(
                  padding: const EdgeInsets.symmetric(horizontal: 4),
                  child: Row(
                    children: [
                      Container(
                        width: 34,
                        height: 34,
                        decoration: BoxDecoration(
                          color: const Color(0xFF18181C),
                          borderRadius: BorderRadius.circular(8),
                          border: Border.all(color: const Color(0xFF27272A)),
                        ),
                        alignment: Alignment.center,
                        child: Icon(
                          _platformIcon(peer),
                          size: 18,
                          color: const Color(0xFFF4F4F5),
                        ),
                      ),
                      const SizedBox(width: 12),
                      Expanded(
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              peer.alias,
                              maxLines: 1,
                              overflow: TextOverflow.ellipsis,
                              style: const TextStyle(
                                color: Color(0xFFF4F4F5),
                                fontSize: 15,
                                fontWeight: FontWeight.w600,
                              ),
                            ),
                            Text(
                              '${peer.os} · ${peer.endpointLabel} (${peer.shortCode})',
                              maxLines: 1,
                              overflow: TextOverflow.ellipsis,
                              style: const TextStyle(
                                color: Color(0xFF71717A),
                                fontSize: 12,
                                fontFamily: 'monospace',
                              ),
                            ),
                          ],
                        ),
                      ),
                    ],
                  ),
                ),
                const SizedBox(height: 14),
                Container(
                  decoration: BoxDecoration(
                    color: const Color(0xFF18181C),
                    borderRadius: BorderRadius.circular(10),
                    border: Border.all(color: const Color(0xFF27272A)),
                  ),
                  clipBehavior: Clip.antiAlias,
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      _buildActionSheetRow(
                        icon: Icons.upload_file_outlined,
                        title: '发送文件',
                        subtitle: '选择一个或多个文件发送',
                        onTap: () {
                          Navigator.of(ctx).pop();
                          _pickAndSendFiles(peer);
                        },
                      ),
                      if (!_isMobilePlatform &&
                          peer.deviceType != DeviceType.web) ...[
                        const Divider(
                          height: 1,
                          thickness: 1,
                          color: Color(0xFF27272A),
                          indent: 46,
                        ),
                        _buildActionSheetRow(
                          icon: Icons.folder_outlined,
                          title: '发送文件夹',
                          subtitle: '递归保持原目录层级传输',
                          onTap: () {
                            Navigator.of(ctx).pop();
                            _pickAndSendFolder(peer);
                          },
                        ),
                      ],
                      const Divider(
                        height: 1,
                        thickness: 1,
                        color: Color(0xFF27272A),
                        indent: 46,
                      ),
                      _buildActionSheetRow(
                        icon: Icons.content_paste_rounded,
                        title: '发送当前剪贴板',
                        subtitle: '直接发送已复制的文本内容',
                        onTap: () {
                          Navigator.of(ctx).pop();
                          _sendClipboardInstantly(peer);
                        },
                      ),
                      const Divider(
                        height: 1,
                        thickness: 1,
                        color: Color(0xFF27272A),
                        indent: 46,
                      ),
                      _buildActionSheetRow(
                        icon: Icons.notes_rounded,
                        title: '发送文本消息',
                        subtitle: '手动输入文本或链接',
                        onTap: () {
                          Navigator.of(ctx).pop();
                          _showSendTextDialog(peer);
                        },
                      ),
                      const Divider(
                        height: 1,
                        thickness: 1,
                        color: Color(0xFF27272A),
                        indent: 46,
                      ),
                      _buildActionSheetRow(
                        icon: isTrusted
                            ? Icons.verified_user_rounded
                            : Icons.verified_user_outlined,
                        title: '信任该设备（免确认接收）',
                        trailing: SizedBox(
                          height: 24,
                          child: Switch(
                            value: isTrusted,
                            onChanged: (v) {
                              _transferService.toggleTrustPeer(peer.id, trust: v);
                              if (mounted) setState(() {});
                              Navigator.of(ctx).pop();
                            },
                          ),
                        ),
                        onTap: () {
                          _transferService.toggleTrustPeer(
                            peer.id,
                            trust: !isTrusted,
                          );
                          if (mounted) setState(() {});
                          Navigator.of(ctx).pop();
                        },
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
        );
      },
    );
  }

  Widget _buildActionSheetRow({
    required IconData icon,
    required String title,
    String? subtitle,
    Widget? trailing,
    required VoidCallback onTap,
  }) {
    return InkWell(
      onTap: onTap,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
        child: Row(
          children: [
            Icon(icon, size: 19, color: const Color(0xFFA1A1AA)),
            const SizedBox(width: 14),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    title,
                    style: const TextStyle(
                      color: Color(0xFFF4F4F5),
                      fontSize: 14,
                      fontWeight: FontWeight.w500,
                    ),
                  ),
                  if (subtitle != null) ...[
                    const SizedBox(height: 1),
                    Text(
                      subtitle,
                      style: const TextStyle(
                        color: Color(0xFF71717A),
                        fontSize: 11.5,
                      ),
                    ),
                  ],
                ],
              ),
            ),
            trailing ??
                const Icon(
                  Icons.chevron_right_rounded,
                  size: 18,
                  color: Color(0xFF52525B),
                ),
          ],
        ),
      ),
    );
  }

  Future<void> _pickAndSendFiles(PeerDevice peer) async {
    try {
      final pickedFiles = await FilePicker.pickFiles();
      if (pickedFiles.isEmpty) return;

      final items = <TransferFileItem>[];
      for (int i = 0; i < pickedFiles.length; i++) {
        final pf = pickedFiles[i];
        final size = (await pf.length()) ?? 0;
        final filePath = pf.path;
        final memBytes = filePath == null ? await pf.readAsBytes() : null;
        items.add(
          TransferFileItem(
            id: '$i-${DateTime.now().millisecondsSinceEpoch}',
            name: pf.name,
            size: size,
            localPath: filePath,
            memoryBytes: memBytes,
          ),
        );
      }

      await _executeSendItems(peer, items);
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('选择或发送文件失败: $e')),
      );
    }
  }

  Future<void> _pickAndSendFolder(PeerDevice peer) async {
    try {
      final dirPath = await FilePicker.getDirectoryPath(
        dialogTitle: '选择要发送给 ${peer.alias} 的文件夹',
      );
      if (dirPath == null || dirPath.trim().isEmpty) return;

      final rootDir = Directory(dirPath);
      if (!rootDir.existsSync()) return;

      final folderName = rootDir.uri.pathSegments
              .where((s) => s.isNotEmpty)
              .lastOrNull ??
          'Folder';

      final items = <TransferFileItem>[];
      int index = 0;
      await for (final entity
          in rootDir.list(recursive: true, followLinks: false)) {
        if (entity is File) {
          final fileName =
              entity.uri.pathSegments.where((s) => s.isNotEmpty).last;
          if (fileName == '.DS_Store') continue;

          final subPath = entity.path
              .substring(rootDir.path.length)
              .replaceAll('\\', '/')
              .replaceFirst(RegExp(r'^/+'), '');
          final relPath = '$folderName/$subPath';
          final size = entity.statSync().size;

          items.add(
            TransferFileItem(
              id: 'dir-$index-${DateTime.now().millisecondsSinceEpoch}',
              name: fileName,
              relativePath: relPath,
              size: size,
              localPath: entity.path,
            ),
          );
          index++;
        }
      }

      if (items.isEmpty) {
        if (!mounted) return;
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('所选文件夹为空')),
        );
        return;
      }

      await _executeSendItems(peer, items);
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('读取文件夹失败: $e')),
      );
    }
  }

  Future<void> _executeSendItems(
    PeerDevice peer,
    List<TransferFileItem> items,
  ) async {
    if (!mounted) return;
    final session = await _transferService.sendFilesToPeer(
      peer: peer,
      files: items,
    );

    if (!mounted) return;
    if (session.status == TransferStatus.completed) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('已发送 ${items.length} 项至 ${peer.alias}'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    } else if (session.status == TransferStatus.rejected) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('${peer.alias} 拒绝了接收请求'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    } else if (session.status == TransferStatus.failed) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('传输失败: ${session.errorMessage ?? "连接中断"}'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    }
  }

  Future<void> _changeSaveDirectory() async {
    try {
      final selectedDir = await FilePicker.getDirectoryPath(
        dialogTitle: '选择接收目录',
        initialDirectory: _transferService.saveDirectoryPath,
      );
      if (selectedDir != null && selectedDir.trim().isNotEmpty) {
        final ok = _transferService.setSaveDirectoryPath(selectedDir);
        if (!mounted) return;
        if (ok) {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Text('接收目录已更新: $selectedDir'),
              behavior: SnackBarBehavior.floating,
            ),
          );
        } else {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Text(
                '系统权限限制无法直接写入该路径，当前目录: ${_transferService.saveDirectoryPath}',
              ),
              behavior: SnackBarBehavior.floating,
            ),
          );
        }
      }
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('无法更改目录: $e')),
      );
    }
  }

  Future<void> _showSendTextDialog(PeerDevice peer) async {
    final text = await showDialog<String>(
      context: context,
      builder: (_) => _TextInputDialog(
        title: '发送文本 · ${peer.alias}',
        hintText: '输入文本或粘贴链接...',
        confirmLabel: '发送',
        maxLines: 4,
        showPasteButton: true,
      ),
    );
    if (text == null || text.trim().isEmpty || !mounted) return;
    final session = await _transferService.sendFilesToPeer(
      peer: peer,
      files: const [],
      textMessage: text.trim(),
    );
    if (!mounted) return;
    if (session.status == TransferStatus.completed) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('文本已发送至 ${peer.alias}'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    } else if (session.status == TransferStatus.rejected) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('${peer.alias} 拒绝了接收请求'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    } else if (session.status == TransferStatus.failed) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('发送失败: ${session.errorMessage ?? "连接中断"}'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    }
  }

  Future<void> _showConnectByCodeDialog() async {
    final input = await showDialog<String>(
      context: context,
      builder: (_) => const _TextInputDialog(
        title: '指定 IP 或短码连接',
        description: '输入目标设备的 IP 尾段短码（如 105）或完整地址（如 192.168.1.105:53318）：',
        hintText: '105 或 192.168.1.105',
        confirmLabel: '连接',
        monospace: true,
      ),
    );
    if (input == null || input.trim().isEmpty || !mounted) return;
    final found = await _discoveryService.connectByCodeOrIp(input.trim());
    if (!mounted) return;
    if (found != null) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('已连接 ${found.alias} (${found.ip})'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    } else {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('未能在该地址探测到在线节点'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    }
  }

  Future<void> _showEditProfileDialog() async {
    final local = _discoveryService.localDevice;
    final newName = await showDialog<String>(
      context: context,
      builder: (_) => _TextInputDialog(
        title: '设备标识设置',
        labelText: '设备名称',
        initialValue: local.alias,
        confirmLabel: '保存',
      ),
    );
    if (newName == null || newName.trim().isEmpty || !mounted) return;
    _discoveryService.updateProfile(
      alias: newName.trim(),
      avatar: local.os.toLowerCase(),
    );
  }

  Future<void> _showWebQrDialog() async {
    final local = _discoveryService.localDevice;
    final webUrl = 'http://${local.ip}:${local.transferPort}';
    await showDialog<void>(
      context: context,
      builder: (ctx) {
        return AlertDialog(
          backgroundColor: const Color(0xFF121215),
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(10),
            side: const BorderSide(color: Color(0xFF27272A)),
          ),
          title: Row(
            children: [
              const Icon(
                Icons.qr_code_2_rounded,
                size: 20,
                color: Color(0xFFF4F4F5),
              ),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  'Web 扫码互传 · ${local.alias}',
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    color: Color(0xFFF4F4F5),
                    fontSize: 15.5,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ),
            ],
          ),
          content: SingleChildScrollView(
            child: SizedBox(
              width: 300,
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  const Text(
                    '同一局域网下的手机或电脑扫码后，即可在浏览器与本机之间双向收发文件与文本',
                    textAlign: TextAlign.center,
                    style: TextStyle(color: Color(0xFFA1A1AA), fontSize: 12.5),
                  ),
                  const SizedBox(height: 16),
                  Container(
                    padding: const EdgeInsets.all(14),
                    decoration: BoxDecoration(
                      color: Colors.white,
                      borderRadius: BorderRadius.circular(10),
                    ),
                    child: QrImageView(
                      data: webUrl,
                      version: QrVersions.auto,
                      size: 196,
                      backgroundColor: Colors.white,
                      eyeStyle: const QrEyeStyle(
                        eyeShape: QrEyeShape.square,
                        color: Color(0xFF09090B),
                      ),
                      dataModuleStyle: const QrDataModuleStyle(
                        dataModuleShape: QrDataModuleShape.square,
                        color: Color(0xFF09090B),
                      ),
                    ),
                  ),
                  const SizedBox(height: 14),
                  Container(
                    width: double.infinity,
                    padding: const EdgeInsets.symmetric(
                      horizontal: 10,
                      vertical: 8,
                    ),
                    decoration: BoxDecoration(
                      color: const Color(0xFF09090B),
                      borderRadius: BorderRadius.circular(6),
                      border: Border.all(color: const Color(0xFF27272A)),
                    ),
                    child: SelectableText(
                      webUrl,
                      textAlign: TextAlign.center,
                      style: const TextStyle(
                        color: Color(0xFFE4E4E7),
                        fontSize: 12.5,
                        fontFamily: 'monospace',
                      ),
                    ),
                  ),
                ],
              ),
            ),
          ),
          actions: [
            TextButton(
              onPressed: () {
                Clipboard.setData(ClipboardData(text: webUrl));
                ScaffoldMessenger.of(context).showSnackBar(
                  SnackBar(
                    content: Text('已复制链接: $webUrl'),
                    behavior: SnackBarBehavior.floating,
                  ),
                );
              },
              child: const Text('复制链接'),
            ),
            FilledButton(
              style: FilledButton.styleFrom(
                backgroundColor: const Color(0xFF2563EB),
                foregroundColor: Colors.white,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(6),
                ),
              ),
              onPressed: () => Navigator.of(ctx).pop(),
              child: const Text('关闭'),
            ),
          ],
        );
      },
    );
  }

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: _discoveryService,
      builder: (context, _) {
        final local = _discoveryService.localDevice;
        final peers = _discoveryService.peers;
        final screenWidth = MediaQuery.sizeOf(context).width;
        final isCompactHeader = screenWidth < 540;
        final isUltraNarrow = screenWidth < 420;

        return Scaffold(
          backgroundColor: const Color(0xFF09090B),
          appBar: AppBar(
            toolbarHeight: 52,
            backgroundColor: const Color(0xFF09090B),
            surfaceTintColor: Colors.transparent,
            elevation: 0,
            scrolledUnderElevation: 0,
            titleSpacing: isCompactHeader ? 14 : 20,
            shape: const Border(
              bottom: BorderSide(color: Color(0xFF27272A), width: 1),
            ),
            title: Row(
              children: [
                ClipRRect(
                  borderRadius: BorderRadius.circular(5),
                  child: Image.asset(
                    'assets/app_icon.png',
                    width: 22,
                    height: 22,
                    filterQuality: FilterQuality.high,
                  ),
                ),
                const SizedBox(width: 8),
                const Text(
                  '邻传',
                  style: TextStyle(
                    color: Color(0xFFF4F4F5),
                    fontSize: 15.5,
                    fontWeight: FontWeight.w600,
                    letterSpacing: 0.2,
                  ),
                ),
                const SizedBox(width: 10),
                Flexible(
                  child: Container(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 8,
                      vertical: 3,
                    ),
                    decoration: BoxDecoration(
                      color: const Color(0xFF121215),
                      borderRadius: BorderRadius.circular(5),
                      border: Border.all(color: const Color(0xFF27272A)),
                    ),
                    child: Text(
                      isUltraNarrow
                          ? local.shortCode
                          : '${local.ip} · ${local.shortCode}',
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(
                        color: Color(0xFFA1A1AA),
                        fontSize: 11.5,
                        fontFamily: 'monospace',
                      ),
                    ),
                  ),
                ),
              ],
            ),
            actions: [
              IconButton(
                visualDensity: VisualDensity.compact,
                tooltip: 'Web 扫码访问',
                onPressed: _showWebQrDialog,
                icon: const Icon(
                  Icons.qr_code_2_rounded,
                  size: 19,
                  color: Color(0xFFA1A1AA),
                ),
              ),
              IconButton(
                visualDensity: VisualDensity.compact,
                tooltip: '指定 IP 或短码连接',
                onPressed: _showConnectByCodeDialog,
                icon: const Icon(
                  Icons.link_rounded,
                  size: 18,
                  color: Color(0xFFA1A1AA),
                ),
              ),
              IconButton(
                visualDensity: VisualDensity.compact,
                tooltip: '扫描当前网段',
                onPressed: _discoveryService.isScanningSubnet
                    ? null
                    : () async {
                        await _discoveryService.scanSubnet();
                      },
                icon: _discoveryService.isScanningSubnet
                    ? const SizedBox(
                        width: 15,
                        height: 15,
                        child: CircularProgressIndicator(
                          strokeWidth: 1.8,
                          color: Color(0xFFA1A1AA),
                        ),
                      )
                    : const Icon(
                        Icons.radar_rounded,
                        size: 18,
                        color: Color(0xFFA1A1AA),
                      ),
              ),
              IconButton(
                visualDensity: VisualDensity.compact,
                tooltip: '设置',
                onPressed: _openSettingsPage,
                icon: const Icon(
                  Icons.settings_outlined,
                  size: 18,
                  color: Color(0xFFA1A1AA),
                ),
              ),
              SizedBox(width: isCompactHeader ? 6 : 12),
            ],
          ),
          body: !_initialized
              ? const Center(
                  child: SizedBox(
                    width: 22,
                    height: 22,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  ),
                )
              : LayoutBuilder(
                  builder: (context, constraints) {
                    final isWide = constraints.maxWidth >= 860;
                    if (isWide) {
                      return Row(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Expanded(
                            child: ListView(
                              padding: const EdgeInsets.fromLTRB(24, 18, 24, 28),
                              children: [
                                _buildOnlinePeersGroup(peers),
                              ],
                            ),
                          ),
                          Container(
                            width: 390,
                            height: double.infinity,
                            decoration: const BoxDecoration(
                              color: Color(0xFF0E0E11),
                              border: Border(
                                left: BorderSide(color: Color(0xFF27272A)),
                              ),
                            ),
                            child: ListView(
                              padding: const EdgeInsets.fromLTRB(18, 18, 18, 28),
                              children: [
                                AnimatedBuilder(
                                  animation: _transferService,
                                  builder: (context, _) => _buildTransfersGroup(
                                    _transferService.sessions,
                                  ),
                                ),
                              ],
                            ),
                          ),
                        ],
                      );
                    }
                    return ListView(
                      padding: const EdgeInsets.fromLTRB(16, 14, 16, 28),
                      children: [
                        _buildOnlinePeersGroup(peers),
                        const SizedBox(height: 22),
                        AnimatedBuilder(
                          animation: _transferService,
                          builder: (context, _) =>
                              _buildTransfersGroup(_transferService.sessions),
                        ),
                      ],
                    );
                  },
                ),
        );
      },
    );
  }

  Future<void> _openSettingsPage() async {
    await Navigator.of(context).push<void>(
      MaterialPageRoute(
        builder: (pageCtx) {
          return AnimatedBuilder(
            animation: Listenable.merge([_discoveryService, _transferService]),
            builder: (context, _) {
              final local = _discoveryService.localDevice;
              return Scaffold(
                backgroundColor: const Color(0xFF09090B),
                appBar: AppBar(
                  toolbarHeight: 52,
                  backgroundColor: const Color(0xFF09090B),
                  surfaceTintColor: Colors.transparent,
                  elevation: 0,
                  scrolledUnderElevation: 0,
                  titleSpacing: 0,
                  shape: const Border(
                    bottom: BorderSide(color: Color(0xFF27272A), width: 1),
                  ),
                  leading: IconButton(
                    visualDensity: VisualDensity.compact,
                    tooltip: '返回',
                    onPressed: () => Navigator.of(pageCtx).pop(),
                    icon: const Icon(
                      Icons.arrow_back_ios_new_rounded,
                      size: 17,
                      color: Color(0xFFF4F4F5),
                    ),
                  ),
                  title: const Text(
                    '设置',
                    style: TextStyle(
                      color: Color(0xFFF4F4F5),
                      fontSize: 15,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
                body: Align(
                  alignment: Alignment.topCenter,
                  child: ConstrainedBox(
                    constraints: const BoxConstraints(maxWidth: 640),
                    child: ListView(
                      padding: const EdgeInsets.fromLTRB(16, 18, 16, 32),
                      children: [
                        _buildLocalSettingsGroup(local),
                      ],
                    ),
                  ),
                ),
              );
            },
          );
        },
      ),
    );
  }

  Widget _buildGroupHeader({
    required String title,
    Widget? trailing,
  }) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(4, 0, 4, 8),
      child: Row(
        children: [
          Text(
            title,
            style: const TextStyle(
              color: Color(0xFF71717A),
              fontSize: 12,
              fontWeight: FontWeight.w500,
            ),
          ),
          const Spacer(),
          ?trailing,
        ],
      ),
    );
  }

  Widget _buildOnlinePeersGroup(List<PeerDevice> peers) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _buildGroupHeader(
          title: '在线设备 (${peers.length})',
          trailing: InkWell(
            onTap: _isRefreshingPeers ? null : _handleRefreshPeers,
            borderRadius: BorderRadius.circular(4),
            child: Padding(
              padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
              child: Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  if (_isRefreshingPeers) ...[
                    const SizedBox(
                      width: 10,
                      height: 10,
                      child: CircularProgressIndicator(
                        strokeWidth: 1.5,
                        color: Color(0xFF3B82F6),
                      ),
                    ),
                    const SizedBox(width: 5),
                  ],
                  Text(
                    _isRefreshingPeers ? '刷新中' : '刷新',
                    style: TextStyle(
                      color: _isRefreshingPeers
                          ? const Color(0xFF71717A)
                          : const Color(0xFF3B82F6),
                      fontSize: 12,
                    ),
                  ),
                ],
              ),
            ),
          ),
        ),
        Container(
          decoration: BoxDecoration(
            color: const Color(0xFF121215),
            borderRadius: BorderRadius.circular(10),
            border: Border.all(color: const Color(0xFF27272A)),
          ),
          clipBehavior: Clip.antiAlias,
          child: peers.isEmpty
              ? InkWell(
                  onTap: _discoveryService.isScanningSubnet
                      ? null
                      : () => _discoveryService.scanSubnet(),
                  child: Padding(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 14,
                      vertical: 14,
                    ),
                    child: Row(
                      children: [
                        Container(
                          width: 32,
                          height: 32,
                          decoration: BoxDecoration(
                            color: const Color(0xFF18181C),
                            borderRadius: BorderRadius.circular(7),
                            border: Border.all(color: const Color(0xFF27272A)),
                          ),
                          alignment: Alignment.center,
                          child: const Icon(
                            Icons.wifi_find_rounded,
                            size: 16,
                            color: Color(0xFF71717A),
                          ),
                        ),
                        const SizedBox(width: 12),
                        const Expanded(
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Text(
                                '暂未发现其他在线设备',
                                style: TextStyle(
                                  color: Color(0xFFE4E4E7),
                                  fontSize: 14,
                                  fontWeight: FontWeight.w500,
                                ),
                              ),
                              SizedBox(height: 2),
                              Text(
                                '点击扫描当前局域网子网，或通过右上角指定 IP 连接',
                                style: TextStyle(
                                  color: Color(0xFF71717A),
                                  fontSize: 12,
                                ),
                              ),
                            ],
                          ),
                        ),
                        const SizedBox(width: 8),
                        const Text(
                          '扫描',
                          style: TextStyle(
                            color: Color(0xFF3B82F6),
                            fontSize: 13,
                          ),
                        ),
                        const SizedBox(width: 2),
                        const Icon(
                          Icons.chevron_right_rounded,
                          size: 18,
                          color: Color(0xFF52525B),
                        ),
                      ],
                    ),
                  ),
                )
              : Column(
                  children: [
                    for (int i = 0; i < peers.length; i++) ...[
                      if (i > 0)
                        const Divider(
                          height: 1,
                          thickness: 1,
                          color: Color(0xFF222226),
                          indent: 58,
                        ),
                      _buildPeerListRow(peers[i]),
                    ],
                  ],
                ),
        ),
      ],
    );
  }

  Widget _buildPeerListRow(PeerDevice peer) {
    final isTrusted = _transferService.isPeerTrusted(peer.id);
    return InkWell(
      onTap: () => _showMorePeerActions(peer),
      onLongPress: () => _showMorePeerActions(peer),
      onSecondaryTap: () => _showMorePeerActions(peer),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 11),
        child: Row(
          children: [
            Container(
              width: 32,
              height: 32,
              decoration: BoxDecoration(
                color: const Color(0xFF18181C),
                borderRadius: BorderRadius.circular(7),
                border: Border.all(color: const Color(0xFF27272A)),
              ),
              alignment: Alignment.center,
              child: Icon(
                _platformIcon(peer),
                size: 17,
                color: const Color(0xFFF4F4F5),
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      Flexible(
                        child: Text(
                          peer.alias,
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(
                            color: Color(0xFFF4F4F5),
                            fontSize: 14.5,
                            fontWeight: FontWeight.w500,
                          ),
                        ),
                      ),
                      if (isTrusted) ...[
                        const SizedBox(width: 6),
                        Container(
                          padding: const EdgeInsets.symmetric(
                            horizontal: 5,
                            vertical: 1,
                          ),
                          decoration: BoxDecoration(
                            color: const Color(0xFF10B981)
                                .withValues(alpha: 0.12),
                            borderRadius: BorderRadius.circular(4),
                          ),
                          child: const Text(
                            '已信任',
                            style: TextStyle(
                              color: Color(0xFF10B981),
                              fontSize: 10.5,
                              fontWeight: FontWeight.w500,
                            ),
                          ),
                        ),
                      ],
                    ],
                  ),
                  const SizedBox(height: 2),
                  Text(
                    '${peer.os} · ${peer.endpointLabel} (${peer.shortCode})',
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                    style: const TextStyle(
                      color: Color(0xFF71717A),
                      fontSize: 12,
                      fontFamily: 'monospace',
                    ),
                  ),
                ],
              ),
            ),
            const SizedBox(width: 6),
            IconButton(
              visualDensity: VisualDensity.compact,
              tooltip: '发送文件',
              onPressed: () => _pickAndSendFiles(peer),
              icon: const Icon(
                Icons.upload_file_outlined,
                size: 17,
                color: Color(0xFFA1A1AA),
              ),
            ),
            IconButton(
              visualDensity: VisualDensity.compact,
              tooltip: '发送剪贴板',
              onPressed: () => _sendClipboardInstantly(peer),
              icon: const Icon(
                Icons.content_paste_rounded,
                size: 16,
                color: Color(0xFF71717A),
              ),
            ),
            const SizedBox(width: 4),
            const Icon(
              Icons.more_horiz_rounded,
              size: 17,
              color: Color(0xFF52525B),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildLocalSettingsGroup(PeerDevice local) {
    final webUrl = 'http://${local.ip}:${local.transferPort}';
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _buildGroupHeader(title: '本机信息'),
        Container(
          decoration: BoxDecoration(
            color: const Color(0xFF121215),
            borderRadius: BorderRadius.circular(10),
            border: Border.all(color: const Color(0xFF27272A)),
          ),
          clipBehavior: Clip.antiAlias,
          child: Column(
            children: [
              _buildSettingsRow(
                icon: _platformIcon(local),
                title: '设备名称',
                valueText: local.alias,
                onTap: _showEditProfileDialog,
              ),
              const Divider(
                height: 1,
                thickness: 1,
                color: Color(0xFF222226),
                indent: 58,
              ),
              _buildSettingsRow(
                icon: Icons.lan_outlined,
                title: '局域网地址',
                valueText: '${local.ip}:${local.transferPort} (${local.shortCode})',
                monospaceValue: true,
                trailing: const Icon(
                  Icons.copy_rounded,
                  size: 15,
                  color: Color(0xFF71717A),
                ),
                onTap: () {
                  final addr = '${local.ip}:${local.transferPort}';
                  Clipboard.setData(ClipboardData(text: addr));
                  ScaffoldMessenger.of(context).showSnackBar(
                    SnackBar(
                      content: Text('已复制本机地址: $addr'),
                      behavior: SnackBarBehavior.floating,
                    ),
                  );
                },
              ),
              const Divider(
                height: 1,
                thickness: 1,
                color: Color(0xFF222226),
                indent: 58,
              ),
              _buildSettingsRow(
                icon: Icons.qr_code_2_rounded,
                title: 'Web 扫码访问',
                subtitle: webUrl,
                valueText: '二维码',
                onTap: _showWebQrDialog,
              ),
            ],
          ),
        ),
        const SizedBox(height: 22),
        _buildGroupHeader(title: '接收设置'),
        Container(
          decoration: BoxDecoration(
            color: const Color(0xFF121215),
            borderRadius: BorderRadius.circular(10),
            border: Border.all(color: const Color(0xFF27272A)),
          ),
          clipBehavior: Clip.antiAlias,
          child: Column(
            children: [
              _buildSettingsRow(
                icon: Icons.folder_outlined,
                title: '接收保存目录',
                subtitle: _transferService.saveDirectoryPath,
                trailing: Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    if (!_isMobilePlatform)
                      IconButton(
                        visualDensity: VisualDensity.compact,
                        tooltip: '打开文件夹',
                        onPressed: () {
                          SystemFileOpener.openDirectory(
                            _transferService.saveDirectoryPath,
                          );
                        },
                        icon: const Icon(
                          Icons.folder_open_rounded,
                          size: 16,
                          color: Color(0xFFA1A1AA),
                        ),
                      ),
                    const Icon(
                      Icons.chevron_right_rounded,
                      size: 18,
                      color: Color(0xFF52525B),
                    ),
                  ],
                ),
                onTap: _changeSaveDirectory,
              ),
              const Divider(
                height: 1,
                thickness: 1,
                color: Color(0xFF222226),
                indent: 58,
              ),
              _buildSettingsRow(
                icon: Icons.bolt_rounded,
                title: '免确认自动接收',
                subtitle: '收到传输请求时直接保存文件',
                trailing: SizedBox(
                  height: 24,
                  child: Switch(
                    value: _transferService.autoAccept,
                    onChanged: _transferService.setAutoAccept,
                  ),
                ),
                onTap: () => _transferService.setAutoAccept(
                  !_transferService.autoAccept,
                ),
              ),
              const Divider(
                height: 1,
                thickness: 1,
                color: Color(0xFF222226),
                indent: 58,
              ),
              _buildSettingsRow(
                icon: Icons.verified_user_outlined,
                title: '已信任设备',
                subtitle: _transferService.trustedPeerCount > 0
                    ? '点击可清空所有已授权免确认的设备'
                    : '在收到传输时可勾选信任指定设备',
                valueText: '${_transferService.trustedPeerCount} 台',
                onTap: _transferService.trustedPeerCount > 0
                    ? _confirmClearTrustedPeers
                    : null,
              ),
            ],
          ),
        ),
      ],
    );
  }

  Future<void> _confirmClearTrustedPeers() async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        backgroundColor: const Color(0xFF121215),
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(10),
          side: const BorderSide(color: Color(0xFF27272A)),
        ),
        title: const Text(
          '清空已信任设备',
          style: TextStyle(color: Color(0xFFF4F4F5), fontSize: 15.5),
        ),
        content: const Text(
          '清空后，此前授权过免确认的设备向本机发送文件时，将重新弹出接收确认提示。',
          style: TextStyle(color: Color(0xFFA1A1AA), fontSize: 13),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(ctx).pop(false),
            child: const Text('取消'),
          ),
          FilledButton(
            style: FilledButton.styleFrom(
              backgroundColor: const Color(0xFFEF4444),
              foregroundColor: Colors.white,
              shape: RoundedRectangleBorder(
                borderRadius: BorderRadius.circular(6),
              ),
            ),
            onPressed: () => Navigator.of(ctx).pop(true),
            child: const Text('清空'),
          ),
        ],
      ),
    );
    if (confirmed == true && mounted) {
      _transferService.clearTrustedPeers();
      setState(() {});
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('已清空所有已信任设备'),
          behavior: SnackBarBehavior.floating,
        ),
      );
    }
  }

  Widget _buildSettingsRow({
    required IconData icon,
    required String title,
    String? subtitle,
    String? valueText,
    bool monospaceValue = false,
    Widget? trailing,
    VoidCallback? onTap,
  }) {
    return InkWell(
      onTap: onTap,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
        child: Row(
          children: [
            Container(
              width: 30,
              height: 30,
              decoration: BoxDecoration(
                color: const Color(0xFF18181C),
                borderRadius: BorderRadius.circular(7),
                border: Border.all(color: const Color(0xFF27272A)),
              ),
              alignment: Alignment.center,
              child: Icon(icon, size: 16, color: const Color(0xFFA1A1AA)),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    title,
                    style: const TextStyle(
                      color: Color(0xFFF4F4F5),
                      fontSize: 14,
                      fontWeight: FontWeight.w400,
                    ),
                  ),
                  if (subtitle != null) ...[
                    const SizedBox(height: 2),
                    Text(
                      subtitle,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(
                        color: Color(0xFF71717A),
                        fontSize: 11.5,
                        fontFamily: 'monospace',
                      ),
                    ),
                  ],
                ],
              ),
            ),
            if (valueText != null) ...[
              const SizedBox(width: 8),
              ConstrainedBox(
                constraints: const BoxConstraints(maxWidth: 220),
                child: Text(
                  valueText,
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  textAlign: TextAlign.right,
                  style: TextStyle(
                    color: const Color(0xFFA1A1AA),
                    fontSize: 13,
                    fontFamily: monospaceValue ? 'monospace' : null,
                  ),
                ),
              ),
            ],
            const SizedBox(width: 4),
            trailing ??
                const Icon(
                  Icons.chevron_right_rounded,
                  size: 18,
                  color: Color(0xFF52525B),
                ),
          ],
        ),
      ),
    );
  }

  Widget _buildTransfersGroup(List<TransferSession> sessions) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _buildGroupHeader(
          title: '传输记录 (${sessions.length})',
          trailing: sessions.isNotEmpty
              ? InkWell(
                  onTap: _transferService.clearHistory,
                  borderRadius: BorderRadius.circular(4),
                  child: const Padding(
                    padding: EdgeInsets.symmetric(horizontal: 6, vertical: 2),
                    child: Text(
                      '清理已完成',
                      style: TextStyle(
                        color: Color(0xFF71717A),
                        fontSize: 12,
                      ),
                    ),
                  ),
                )
              : null,
        ),
        Container(
          decoration: BoxDecoration(
            color: const Color(0xFF121215),
            borderRadius: BorderRadius.circular(10),
            border: Border.all(color: const Color(0xFF27272A)),
          ),
          clipBehavior: Clip.antiAlias,
          child: sessions.isEmpty
              ? Padding(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 14,
                    vertical: 14,
                  ),
                  child: Row(
                    children: [
                      Container(
                        width: 32,
                        height: 32,
                        decoration: BoxDecoration(
                          color: const Color(0xFF18181C),
                          borderRadius: BorderRadius.circular(7),
                          border: Border.all(color: const Color(0xFF27272A)),
                        ),
                        alignment: Alignment.center,
                        child: const Icon(
                          Icons.swap_horiz_rounded,
                          size: 16,
                          color: Color(0xFF71717A),
                        ),
                      ),
                      const SizedBox(width: 12),
                      const Text(
                        '暂无传输记录',
                        style: TextStyle(
                          color: Color(0xFF71717A),
                          fontSize: 13.5,
                        ),
                      ),
                    ],
                  ),
                )
              : Column(
                  children: [
                    for (int i = 0; i < sessions.length; i++) ...[
                      if (i > 0)
                        const Divider(
                          height: 1,
                          thickness: 1,
                          color: Color(0xFF222226),
                          indent: 58,
                        ),
                      _buildSessionRow(sessions[i]),
                    ],
                  ],
                ),
        ),
      ],
    );
  }

  Future<void> _previewOrOpenFile(String filePath) async {
    if (!_isMobilePlatform) {
      await SystemFileOpener.openFile(filePath);
      return;
    }

    final file = File(filePath);
    if (!file.existsSync()) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('文件不存在或已被移除')),
      );
      return;
    }

    final fileName =
        file.uri.pathSegments.where((s) => s.isNotEmpty).lastOrNull ?? filePath;
    final lower = fileName.toLowerCase();
    final isImage = lower.endsWith('.png') ||
        lower.endsWith('.jpg') ||
        lower.endsWith('.jpeg') ||
        lower.endsWith('.gif') ||
        lower.endsWith('.webp') ||
        lower.endsWith('.bmp');
    final isText = lower.endsWith('.txt') ||
        lower.endsWith('.md') ||
        lower.endsWith('.json') ||
        lower.endsWith('.yaml') ||
        lower.endsWith('.yml') ||
        lower.endsWith('.xml') ||
        lower.endsWith('.csv') ||
        lower.endsWith('.log') ||
        lower.endsWith('.dart') ||
        lower.endsWith('.py') ||
        lower.endsWith('.js') ||
        lower.endsWith('.ts') ||
        lower.endsWith('.java') ||
        lower.endsWith('.kt') ||
        lower.endsWith('.swift') ||
        lower.endsWith('.html') ||
        lower.endsWith('.css') ||
        lower.endsWith('.sh');

    String? textPreview;
    final fileSize = file.lengthSync();
    if (isText && fileSize <= 512 * 1024) {
      try {
        textPreview = utf8.decode(file.readAsBytesSync(), allowMalformed: true);
      } catch (_) {}
    }

    if (!mounted) return;
    await showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      backgroundColor: const Color(0xFF121215),
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(12)),
        side: BorderSide(color: Color(0xFF27272A)),
      ),
      builder: (ctx) {
        return SafeArea(
          child: ConstrainedBox(
            constraints: BoxConstraints(
              maxHeight: MediaQuery.sizeOf(ctx).height * 0.82,
            ),
            child: Padding(
              padding: const EdgeInsets.fromLTRB(20, 16, 20, 20),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      const Icon(
                        Icons.insert_drive_file_outlined,
                        size: 18,
                        color: Color(0xFFA1A1AA),
                      ),
                      const SizedBox(width: 10),
                      Expanded(
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              fileName,
                              maxLines: 1,
                              overflow: TextOverflow.ellipsis,
                              style: const TextStyle(
                                color: Color(0xFFF4F4F5),
                                fontSize: 14.5,
                                fontWeight: FontWeight.w600,
                              ),
                            ),
                            Text(
                              TransferSession.formatBytes(fileSize),
                              style: const TextStyle(
                                color: Color(0xFF71717A),
                                fontSize: 11.5,
                                fontFamily: 'monospace',
                              ),
                            ),
                          ],
                        ),
                      ),
                      IconButton(
                        onPressed: () => Navigator.of(ctx).pop(),
                        icon: const Icon(
                          Icons.close,
                          size: 18,
                          color: Color(0xFF71717A),
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 12),
                  if (isImage)
                    Flexible(
                      child: Container(
                        width: double.infinity,
                        decoration: BoxDecoration(
                          color: const Color(0xFF09090B),
                          borderRadius: BorderRadius.circular(8),
                          border: Border.all(color: const Color(0xFF27272A)),
                        ),
                        clipBehavior: Clip.antiAlias,
                        child: InteractiveViewer(
                          child: Image.file(file, fit: BoxFit.contain),
                        ),
                      ),
                    )
                  else if (textPreview != null)
                    Flexible(
                      child: Container(
                        width: double.infinity,
                        padding: const EdgeInsets.all(12),
                        decoration: BoxDecoration(
                          color: const Color(0xFF09090B),
                          borderRadius: BorderRadius.circular(8),
                          border: Border.all(color: const Color(0xFF27272A)),
                        ),
                        child: SingleChildScrollView(
                          child: SelectableText(
                            textPreview,
                            style: const TextStyle(
                              color: Color(0xFFE4E4E7),
                              fontSize: 12.5,
                              fontFamily: 'monospace',
                            ),
                          ),
                        ),
                      ),
                    )
                  else
                    Container(
                      width: double.infinity,
                      padding: const EdgeInsets.all(12),
                      decoration: BoxDecoration(
                        color: const Color(0xFF09090B),
                        borderRadius: BorderRadius.circular(8),
                        border: Border.all(color: const Color(0xFF27272A)),
                      ),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          const Text(
                            '存储路径',
                            style: TextStyle(
                              color: Color(0xFF71717A),
                              fontSize: 11.5,
                            ),
                          ),
                          const SizedBox(height: 4),
                          SelectableText(
                            filePath,
                            style: const TextStyle(
                              color: Color(0xFFE4E4E7),
                              fontSize: 12,
                              fontFamily: 'monospace',
                            ),
                          ),
                        ],
                      ),
                    ),
                  const SizedBox(height: 14),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.end,
                    children: [
                      if (textPreview != null) ...[
                        OutlinedButton(
                          style: OutlinedButton.styleFrom(
                            foregroundColor: const Color(0xFFE4E4E7),
                            side: const BorderSide(color: Color(0xFF27272A)),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(6),
                            ),
                          ),
                          onPressed: () {
                            Clipboard.setData(ClipboardData(text: textPreview!));
                            ScaffoldMessenger.of(context).showSnackBar(
                              const SnackBar(content: Text('已复制内容')),
                            );
                          },
                          child: const Text('复制全文'),
                        ),
                        const SizedBox(width: 8),
                      ],
                      FilledButton(
                        style: FilledButton.styleFrom(
                          backgroundColor: const Color(0xFF2563EB),
                          foregroundColor: Colors.white,
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(6),
                          ),
                        ),
                        onPressed: () {
                          Clipboard.setData(ClipboardData(text: filePath));
                          Navigator.of(ctx).pop();
                          ScaffoldMessenger.of(context).showSnackBar(
                            const SnackBar(content: Text('已复制文件路径')),
                          );
                        },
                        child: const Text('复制路径'),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
        );
      },
    );
  }

  String _formatSessionTime(DateTime dt) {
    final h = dt.hour.toString().padLeft(2, '0');
    final m = dt.minute.toString().padLeft(2, '0');
    return '$h:$m';
  }

  IconData _sessionIcon(TransferSession s) {
    if (s.files.isEmpty) return Icons.notes_rounded;
    final hasFolder =
        s.files.any((f) => (f.relativePath ?? '').contains('/'));
    if (hasFolder) return Icons.folder_outlined;
    if (s.files.length > 1) return Icons.file_copy_outlined;
    return Icons.insert_drive_file_outlined;
  }

  Widget _buildCompactActionChip({
    required String label,
    required Color color,
    required VoidCallback onTap,
  }) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(5),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
        child: Text(
          label,
          style: TextStyle(
            color: color,
            fontSize: 12,
            fontWeight: FontWeight.w500,
          ),
        ),
      ),
    );
  }

  Widget _buildSessionRow(TransferSession s) {
    final isSending = s.direction == TransferDirection.sending;
    final isPureText = s.files.isEmpty &&
        s.textMessage != null &&
        s.textMessage!.isNotEmpty;

    final statusLabel = switch (s.status) {
      TransferStatus.waitingConfirmation => '等待确认',
      TransferStatus.transferring =>
        '${(s.progress * 100).toStringAsFixed(0)}%',
      TransferStatus.completed => '已完成',
      TransferStatus.rejected => '已拒绝',
      TransferStatus.cancelled => '已取消',
      TransferStatus.failed => '失败',
    };

    final statusColor = switch (s.status) {
      TransferStatus.waitingConfirmation => const Color(0xFFF59E0B),
      TransferStatus.transferring => const Color(0xFF3B82F6),
      TransferStatus.completed => const Color(0xFF10B981),
      TransferStatus.rejected => const Color(0xFFEF4444),
      TransferStatus.cancelled => const Color(0xFF71717A),
      TransferStatus.failed => const Color(0xFFEF4444),
    };

    final title = s.files.isNotEmpty
        ? (s.files.length == 1
            ? (s.files.first.relativePath ?? s.files.first.name)
            : '${s.files.first.name} 等 ${s.files.length} 个文件')
        : (s.textMessage?.replaceAll('\n', ' ') ?? '文本消息');

    final metaParts = <String>[
      '${isSending ? "↑" : "↓"} ${s.peerAlias}',
      if (s.totalBytes > 0) TransferSession.formatBytes(s.totalBytes),
      if (s.totalBytes > 0 &&
          s.speedBytesPerSec > 0 &&
          (s.status == TransferStatus.transferring ||
              s.status == TransferStatus.completed))
        TransferSession.formatSpeed(s.speedBytesPerSec),
      _formatSessionTime(s.createdAt),
    ];

    VoidCallback? rowTap;
    if (s.savedPaths.isNotEmpty) {
      rowTap = () {
        if (s.savedPaths.length == 1) {
          _previewOrOpenFile(s.savedPaths.first);
        } else {
          _showReceivedFilesSheet(s);
        }
      };
    } else if (isPureText) {
      rowTap = () {
        Clipboard.setData(ClipboardData(text: s.textMessage!));
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('已复制文本到剪贴板'),
            behavior: SnackBarBehavior.floating,
            duration: Duration(seconds: 1),
          ),
        );
      };
    }

    return InkWell(
      onTap: rowTap,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 11),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Container(
              width: 32,
              height: 32,
              margin: const EdgeInsets.only(top: 1),
              decoration: BoxDecoration(
                color: const Color(0xFF18181C),
                borderRadius: BorderRadius.circular(7),
                border: Border.all(color: const Color(0xFF27272A)),
              ),
              alignment: Alignment.center,
              child: Icon(
                _sessionIcon(s),
                size: 16,
                color: const Color(0xFFA1A1AA),
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      Expanded(
                        child: Text(
                          title,
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(
                            color: Color(0xFFF4F4F5),
                            fontSize: 14,
                            fontWeight: FontWeight.w500,
                          ),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Container(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 6,
                          vertical: 1.5,
                        ),
                        decoration: BoxDecoration(
                          color: statusColor.withValues(alpha: 0.12),
                          borderRadius: BorderRadius.circular(4),
                        ),
                        child: Text(
                          statusLabel,
                          style: TextStyle(
                            color: statusColor,
                            fontSize: 11,
                            fontFamily: 'monospace',
                            fontWeight: FontWeight.w500,
                          ),
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 4),
                  Row(
                    children: [
                      Expanded(
                        child: Text(
                          metaParts.join(' · '),
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(
                            color: Color(0xFF71717A),
                            fontSize: 11.5,
                            fontFamily: 'monospace',
                          ),
                        ),
                      ),
                      if (s.isActive)
                        _buildCompactActionChip(
                          label: '取消',
                          color: const Color(0xFFEF4444),
                          onTap: () =>
                              _transferService.cancelSession(s.sessionId),
                        ),
                      if (s.canRetry)
                        _buildCompactActionChip(
                          label: '重试',
                          color: const Color(0xFF3B82F6),
                          onTap: () => _transferService.retrySession(s),
                        ),
                      if (s.textMessage != null && s.textMessage!.isNotEmpty)
                        _buildCompactActionChip(
                          label: '复制',
                          color: isPureText
                              ? const Color(0xFF3B82F6)
                              : const Color(0xFFA1A1AA),
                          onTap: () {
                            Clipboard.setData(
                              ClipboardData(text: s.textMessage!),
                            );
                            ScaffoldMessenger.of(context).showSnackBar(
                              const SnackBar(
                                content: Text('已复制到剪贴板'),
                                behavior: SnackBarBehavior.floating,
                                duration: Duration(seconds: 1),
                              ),
                            );
                          },
                        ),
                      if (s.savedPaths.isNotEmpty) ...[
                        _buildCompactActionChip(
                          label: s.savedPaths.length > 1
                              ? '列表(${s.savedPaths.length})'
                              : (_isMobilePlatform ? '查看' : '打开'),
                          color: const Color(0xFF3B82F6),
                          onTap: () {
                            if (s.savedPaths.length == 1) {
                              _previewOrOpenFile(s.savedPaths.first);
                            } else {
                              _showReceivedFilesSheet(s);
                            }
                          },
                        ),
                        if (!_isMobilePlatform)
                          _buildCompactActionChip(
                            label: '定位',
                            color: const Color(0xFFA1A1AA),
                            onTap: () {
                              SystemFileOpener.revealInFolder(
                                s.savedPaths.first,
                              );
                            },
                          ),
                      ],
                    ],
                  ),
                  if (s.status == TransferStatus.transferring) ...[
                    const SizedBox(height: 7),
                    ClipRRect(
                      borderRadius: BorderRadius.circular(2),
                      child: LinearProgressIndicator(
                        value: s.progress,
                        minHeight: 3,
                        backgroundColor: const Color(0xFF27272A),
                        color: const Color(0xFF3B82F6),
                      ),
                    ),
                  ],
                  if (s.files.isNotEmpty &&
                      s.textMessage != null &&
                      s.textMessage!.isNotEmpty) ...[
                    const SizedBox(height: 5),
                    Text(
                      '附言: ${s.textMessage!.replaceAll('\n', ' ')}',
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(
                        color: Color(0xFFA1A1AA),
                        fontSize: 11.5,
                      ),
                    ),
                  ],
                  if (s.status == TransferStatus.failed &&
                      s.errorMessage != null &&
                      s.errorMessage!.isNotEmpty) ...[
                    const SizedBox(height: 4),
                    Text(
                      s.errorMessage!,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(
                        color: Color(0xFFEF4444),
                        fontSize: 11,
                      ),
                    ),
                  ],
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  Future<void> _showReceivedFilesSheet(TransferSession session) async {
    await showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      backgroundColor: const Color(0xFF121215),
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(14)),
        side: BorderSide(color: Color(0xFF27272A)),
      ),
      builder: (ctx) {
        return SafeArea(
          child: ConstrainedBox(
            constraints: BoxConstraints(
              maxHeight: MediaQuery.sizeOf(ctx).height * 0.78,
            ),
            child: Padding(
              padding: const EdgeInsets.fromLTRB(16, 16, 16, 20),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Padding(
                    padding: const EdgeInsets.symmetric(horizontal: 4),
                    child: Row(
                      children: [
                        const Icon(
                          Icons.folder_copy_outlined,
                          size: 18,
                          color: Color(0xFFA1A1AA),
                        ),
                        const SizedBox(width: 10),
                        Expanded(
                          child: Text(
                            '已接收文件 (${session.savedPaths.length})',
                            style: const TextStyle(
                              color: Color(0xFFF4F4F5),
                              fontSize: 15,
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                        ),
                        IconButton(
                          visualDensity: VisualDensity.compact,
                          onPressed: () => Navigator.of(ctx).pop(),
                          icon: const Icon(
                            Icons.close,
                            size: 18,
                            color: Color(0xFF71717A),
                          ),
                        ),
                      ],
                    ),
                  ),
                  const SizedBox(height: 10),
                  Flexible(
                    child: Container(
                      decoration: BoxDecoration(
                        color: const Color(0xFF18181C),
                        borderRadius: BorderRadius.circular(10),
                        border: Border.all(color: const Color(0xFF27272A)),
                      ),
                      clipBehavior: Clip.antiAlias,
                      child: ListView.separated(
                        shrinkWrap: true,
                        itemCount: session.savedPaths.length,
                        separatorBuilder: (_, _) => const Divider(
                          height: 1,
                          thickness: 1,
                          color: Color(0xFF27272A),
                          indent: 42,
                        ),
                        itemBuilder: (_, index) {
                          final path = session.savedPaths[index];
                          final name = path
                              .replaceAll('\\', '/')
                              .split('/')
                              .where((s) => s.isNotEmpty)
                              .lastOrNull ??
                              path;
                          return Padding(
                            padding: const EdgeInsets.symmetric(
                              horizontal: 14,
                              vertical: 10,
                            ),
                            child: Row(
                              children: [
                                const Icon(
                                  Icons.insert_drive_file_outlined,
                                  size: 16,
                                  color: Color(0xFF71717A),
                                ),
                                const SizedBox(width: 12),
                                Expanded(
                                  child: Column(
                                    crossAxisAlignment:
                                        CrossAxisAlignment.start,
                                    children: [
                                      Text(
                                        name,
                                        maxLines: 1,
                                        overflow: TextOverflow.ellipsis,
                                        style: const TextStyle(
                                          color: Color(0xFFF4F4F5),
                                          fontSize: 13.5,
                                          fontWeight: FontWeight.w500,
                                        ),
                                      ),
                                      const SizedBox(height: 2),
                                      Text(
                                        path,
                                        maxLines: 1,
                                        overflow: TextOverflow.ellipsis,
                                        style: const TextStyle(
                                          color: Color(0xFF71717A),
                                          fontSize: 11,
                                          fontFamily: 'monospace',
                                        ),
                                      ),
                                    ],
                                  ),
                                ),
                                TextButton(
                                  style: TextButton.styleFrom(
                                    visualDensity: VisualDensity.compact,
                                    padding: const EdgeInsets.symmetric(
                                      horizontal: 8,
                                    ),
                                    foregroundColor: const Color(0xFF3B82F6),
                                  ),
                                  onPressed: () {
                                    Navigator.of(ctx).pop();
                                    _previewOrOpenFile(path);
                                  },
                                  child: Text(
                                    _isMobilePlatform ? '查看' : '打开',
                                    style: const TextStyle(fontSize: 12),
                                  ),
                                ),
                                if (!_isMobilePlatform)
                                  TextButton(
                                    style: TextButton.styleFrom(
                                      visualDensity: VisualDensity.compact,
                                      padding: const EdgeInsets.symmetric(
                                        horizontal: 8,
                                      ),
                                      foregroundColor: const Color(0xFFA1A1AA),
                                    ),
                                    onPressed: () {
                                      SystemFileOpener.revealInFolder(path);
                                    },
                                    child: const Text(
                                      '定位',
                                      style: TextStyle(fontSize: 12),
                                    ),
                                  ),
                              ],
                            ),
                          );
                        },
                      ),
                    ),
                  ),
                ],
              ),
            ),
          ),
        );
      },
    );
  }
}

class _TextInputDialog extends StatefulWidget {
  final String title;
  final String? description;
  final String? labelText;
  final String? hintText;
  final String initialValue;
  final String confirmLabel;
  final int maxLines;
  final bool monospace;
  final bool showPasteButton;

  const _TextInputDialog({
    required this.title,
    this.description,
    this.labelText,
    this.hintText,
    this.initialValue = '',
    required this.confirmLabel,
    this.maxLines = 1,
    this.monospace = false,
    this.showPasteButton = false,
  });

  @override
  State<_TextInputDialog> createState() => _TextInputDialogState();
}

class _TextInputDialogState extends State<_TextInputDialog> {
  late final TextEditingController _controller;

  @override
  void initState() {
    super.initState();
    _controller = TextEditingController(text: widget.initialValue);
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  void _submit() {
    final text = _controller.text.trim();
    if (text.isEmpty) return;
    Navigator.of(context).pop(text);
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      backgroundColor: const Color(0xFF121215),
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(10),
        side: const BorderSide(color: Color(0xFF27272A)),
      ),
      title: Text(
        widget.title,
        style: const TextStyle(color: Color(0xFFF4F4F5), fontSize: 15.5),
      ),
      content: SingleChildScrollView(
        child: SizedBox(
          width: 360,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              if (widget.description != null) ...[
                Text(
                  widget.description!,
                  style: const TextStyle(
                    color: Color(0xFFA1A1AA),
                    fontSize: 12.5,
                  ),
                ),
                const SizedBox(height: 12),
              ],
              TextField(
                controller: _controller,
                maxLines: widget.maxLines,
                autofocus: true,
                onSubmitted: widget.maxLines == 1 ? (_) => _submit() : null,
                style: TextStyle(
                  color: const Color(0xFFF4F4F5),
                  fontSize: 13.5,
                  fontFamily: widget.monospace ? 'monospace' : null,
                ),
                decoration: InputDecoration(
                  labelText: widget.labelText,
                  labelStyle: const TextStyle(color: Color(0xFFA1A1AA)),
                  hintText: widget.hintText,
                  hintStyle: const TextStyle(color: Color(0xFF71717A)),
                  filled: true,
                  fillColor: const Color(0xFF09090B),
                  enabledBorder: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(8),
                    borderSide: const BorderSide(color: Color(0xFF27272A)),
                  ),
                  focusedBorder: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(8),
                    borderSide: const BorderSide(color: Color(0xFF3B82F6)),
                  ),
                ),
              ),
            ],
          ),
        ),
      ),
      actions: [
        if (widget.showPasteButton)
          TextButton(
            onPressed: () async {
              final data = await Clipboard.getData(Clipboard.kTextPlain);
              if (data?.text != null && mounted) {
                _controller.text = data!.text!;
              }
            },
            child: const Text('粘贴'),
          ),
        TextButton(
          onPressed: () => Navigator.of(context).pop(),
          child: const Text('取消'),
        ),
        FilledButton(
          style: FilledButton.styleFrom(
            backgroundColor: const Color(0xFF2563EB),
            foregroundColor: Colors.white,
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(6),
            ),
          ),
          onPressed: _submit,
          child: Text(widget.confirmLabel),
        ),
      ],
    );
  }
}
