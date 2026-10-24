import 'dart:convert';
import 'dart:io';

class PersistedSettings {
  final String? deviceId;
  final String? alias;
  final String? avatar;
  final bool autoAccept;
  final String? saveDirectoryPath;
  final Set<String> trustedPeerIds;

  const PersistedSettings({
    this.deviceId,
    this.alias,
    this.avatar,
    this.autoAccept = false,
    this.saveDirectoryPath,
    this.trustedPeerIds = const {},
  });

  PersistedSettings copyWith({
    String? deviceId,
    String? alias,
    String? avatar,
    bool? autoAccept,
    String? saveDirectoryPath,
    Set<String>? trustedPeerIds,
  }) {
    return PersistedSettings(
      deviceId: deviceId ?? this.deviceId,
      alias: alias ?? this.alias,
      avatar: avatar ?? this.avatar,
      autoAccept: autoAccept ?? this.autoAccept,
      saveDirectoryPath: saveDirectoryPath ?? this.saveDirectoryPath,
      trustedPeerIds: trustedPeerIds ?? this.trustedPeerIds,
    );
  }

  Map<String, dynamic> toJson() => {
        'deviceId': deviceId,
        'alias': alias,
        'avatar': avatar,
        'autoAccept': autoAccept,
        'saveDirectoryPath': saveDirectoryPath,
        'trustedPeerIds': trustedPeerIds.toList(),
      };

  factory PersistedSettings.fromJson(Map<String, dynamic> json) {
    final rawTrusted = (json['trustedPeerIds'] as List<dynamic>?) ?? const [];
    return PersistedSettings(
      deviceId: json['deviceId'] as String?,
      alias: json['alias'] as String?,
      avatar: json['avatar'] as String?,
      autoAccept: json['autoAccept'] as bool? ?? false,
      saveDirectoryPath: json['saveDirectoryPath'] as String?,
      trustedPeerIds: rawTrusted.map((e) => e.toString()).toSet(),
    );
  }
}

class SettingsService {
  static File _resolveConfigFile() {
    try {
      final home = Platform.environment['HOME'] ??
          Platform.environment['USERPROFILE'];
      if (home != null && home.isNotEmpty) {
        final docs = Directory('$home${Platform.pathSeparator}Documents');
        if (Platform.isIOS && docs.existsSync()) {
          return File(
            '${docs.path}${Platform.pathSeparator}.connections_settings.json',
          );
        }
        return File('$home${Platform.pathSeparator}.connections_settings.json');
      }
      // Android 端使用应用私有持久化目录 (/data/user/0/<pkg>/files)
      final appRoot = Directory.systemTemp.parent;
      final filesDir =
          Directory('${appRoot.path}${Platform.pathSeparator}files');
      if (!filesDir.existsSync()) {
        filesDir.createSync(recursive: true);
      }
      return File(
        '${filesDir.path}${Platform.pathSeparator}connections_settings.json',
      );
    } catch (_) {}
    return File(
      '${Directory.systemTemp.path}${Platform.pathSeparator}connections_settings.json',
    );
  }

  static PersistedSettings load() {
    try {
      final file = _resolveConfigFile();
      if (file.existsSync()) {
        final content = file.readAsStringSync();
        final map = jsonDecode(content) as Map<String, dynamic>;
        return PersistedSettings.fromJson(map);
      }
    } catch (_) {}
    return const PersistedSettings();
  }

  static Future<void> save(PersistedSettings settings) async {
    try {
      final file = _resolveConfigFile();
      await file.writeAsString(
        const JsonEncoder.withIndent('  ').convert(settings.toJson()),
      );
    } catch (_) {}
  }
}
