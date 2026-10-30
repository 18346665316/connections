import 'dart:io';

enum DeviceType {
  desktop,
  mobile,
  tablet,
  web,
}

class PeerDevice {
  final String id;
  final String alias;
  final String avatar;
  final DeviceType deviceType;
  final String os;
  final String ip;
  final List<String> addresses;
  final int transferPort;
  final DateTime lastSeen;

  const PeerDevice({
    required this.id,
    required this.alias,
    required this.avatar,
    required this.deviceType,
    required this.os,
    required this.ip,
    this.addresses = const [],
    required this.transferPort,
    required this.lastSeen,
  });

  /// 返回按优先级排列的候选可达 IP 列表（主 IP 优先，其余多网卡地址随后）
  List<String> get candidateIps {
    final result = <String>[];
    if (ip.isNotEmpty && ip != '0.0.0.0') {
      result.add(ip);
    }
    for (final addr in addresses) {
      final trimmed = addr.trim();
      if (trimmed.isNotEmpty &&
          trimmed != '0.0.0.0' &&
          !result.contains(trimmed)) {
        result.add(trimmed);
      }
    }
    return result;
  }

  /// 提取 IP 最后一段作为局域网短码（例如 192.168.1.105 -> #105）
  String get shortCode {
    final parts = ip.split('.');
    if (parts.length == 4) {
      return '#${parts.last}';
    }
    return '#$transferPort';
  }

  String get endpointLabel =>
      transferPort > 0 ? '$ip:$transferPort' : '$ip · Web';

  PeerDevice copyWith({
    String? id,
    String? alias,
    String? avatar,
    DeviceType? deviceType,
    String? os,
    String? ip,
    List<String>? addresses,
    int? transferPort,
    DateTime? lastSeen,
  }) {
    return PeerDevice(
      id: id ?? this.id,
      alias: alias ?? this.alias,
      avatar: avatar ?? this.avatar,
      deviceType: deviceType ?? this.deviceType,
      os: os ?? this.os,
      ip: ip ?? this.ip,
      addresses: addresses ?? this.addresses,
      transferPort: transferPort ?? this.transferPort,
      lastSeen: lastSeen ?? this.lastSeen,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'alias': alias,
      'avatar': avatar,
      'deviceType': deviceType.name,
      'os': os,
      'ip': ip,
      if (addresses.isNotEmpty) 'addresses': addresses,
      'transferPort': transferPort,
    };
  }

  factory PeerDevice.fromJson(Map<String, dynamic> json, {String? fallbackIp}) {
    final typeStr = (json['deviceType'] as String?) ?? 'desktop';
    final deviceType = DeviceType.values.firstWhere(
      (e) => e.name == typeStr,
      orElse: () => DeviceType.desktop,
    );
    final rawIp = (json['ip'] as String?)?.trim();
    final effectiveIp = (rawIp != null && rawIp.isNotEmpty && rawIp != '0.0.0.0')
        ? rawIp
        : (fallbackIp ?? '127.0.0.1');
    final rawAddresses = (json['addresses'] as List<dynamic>?)
            ?.whereType<String>()
            .map((e) => e.trim())
            .where((e) => e.isNotEmpty && e != '0.0.0.0')
            .toList() ??
        const <String>[];

    return PeerDevice(
      id: json['id'] as String? ?? 'unknown',
      alias: json['alias'] as String? ?? '未知设备',
      avatar: json['avatar'] as String? ?? 'desktop',
      deviceType: deviceType,
      os: json['os'] as String? ?? 'Unknown',
      ip: effectiveIp,
      addresses: rawAddresses,
      transferPort: (json['transferPort'] as num?)?.toInt() ?? 53318,
      lastSeen: DateTime.now(),
    );
  }

  static DeviceType detectLocalDeviceType() {
    if (Platform.isAndroid || Platform.isIOS) {
      return DeviceType.mobile;
    }
    return DeviceType.desktop;
  }

  static String detectLocalOsName() {
    if (Platform.isMacOS) return 'macOS';
    if (Platform.isWindows) return 'Windows';
    if (Platform.isLinux) return 'Linux';
    if (Platform.isIOS) return 'iOS';
    if (Platform.isAndroid) return 'Android';
    return Platform.operatingSystem;
  }
}
