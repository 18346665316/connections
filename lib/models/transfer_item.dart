import 'peer_device.dart';

enum TransferDirection {
  sending,
  receiving,
}

enum TransferStatus {
  waitingConfirmation,
  transferring,
  completed,
  rejected,
  cancelled,
  failed,
}

class TransferFileItem {
  final String id;
  final String name;
  final int size;
  final String? relativePath;
  final String? localPath;
  final List<int>? memoryBytes;

  const TransferFileItem({
    required this.id,
    required this.name,
    required this.size,
    this.relativePath,
    this.localPath,
    this.memoryBytes,
  });

  Map<String, dynamic> toJson() => {
        'id': id,
        'name': name,
        'size': size,
        if (relativePath != null) 'relativePath': relativePath,
      };

  factory TransferFileItem.fromJson(Map<String, dynamic> json) {
    return TransferFileItem(
      id: json['id'] as String? ?? '0',
      name: json['name'] as String? ?? 'unnamed_file',
      size: (json['size'] as num?)?.toInt() ?? 0,
      relativePath: json['relativePath'] as String?,
    );
  }
}

class TransferSession {
  final String sessionId;
  final String peerId;
  final String peerAlias;
  final String peerAvatar;
  final String peerIp;
  final PeerDevice? targetPeer;
  final TransferDirection direction;
  final List<TransferFileItem> files;
  final String? textMessage;
  final DateTime createdAt;

  TransferStatus status;
  int transferredBytes;
  int totalBytes;
  double speedBytesPerSec;
  List<String> savedPaths;
  String? errorMessage;

  TransferSession({
    required this.sessionId,
    required this.peerId,
    required this.peerAlias,
    required this.peerAvatar,
    required this.peerIp,
    this.targetPeer,
    required this.direction,
    required this.files,
    this.textMessage,
    DateTime? createdAt,
    this.status = TransferStatus.waitingConfirmation,
    this.transferredBytes = 0,
    required this.totalBytes,
    this.speedBytesPerSec = 0,
    List<String>? savedPaths,
    this.errorMessage,
  })  : createdAt = createdAt ?? DateTime.now(),
        savedPaths = savedPaths ?? [];

  bool get isActive =>
      status == TransferStatus.waitingConfirmation ||
      status == TransferStatus.transferring;

  bool get canRetry =>
      direction == TransferDirection.sending &&
      targetPeer != null &&
      (status == TransferStatus.failed ||
          status == TransferStatus.rejected ||
          status == TransferStatus.cancelled);

  double get progress {
    if (totalBytes <= 0) {
      return status == TransferStatus.completed ? 1.0 : 0.0;
    }
    return (transferredBytes / totalBytes).clamp(0.0, 1.0);
  }

  static String formatBytes(int bytes) {
    if (bytes < 1024) return '$bytes B';
    if (bytes < 1024 * 1024) {
      return '${(bytes / 1024).toStringAsFixed(1)} KB';
    }
    if (bytes < 1024 * 1024 * 1024) {
      return '${(bytes / (1024 * 1024)).toStringAsFixed(2)} MB';
    }
    return '${(bytes / (1024 * 1024 * 1024)).toStringAsFixed(2)} GB';
  }

  static String formatSpeed(double bytesPerSec) {
    if (bytesPerSec <= 0) return '0 B/s';
    return '${formatBytes(bytesPerSec.round())}/s';
  }
}
