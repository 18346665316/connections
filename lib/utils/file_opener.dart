import 'dart:io';

class SystemFileOpener {
  /// 使用系统默认应用直接打开文件
  static Future<bool> openFile(String filePath) async {
    try {
      final file = File(filePath);
      if (!file.existsSync()) return false;

      if (Platform.isMacOS) {
        final res = await Process.run('open', [filePath]);
        return res.exitCode == 0;
      } else if (Platform.isWindows) {
        final res = await Process.run('cmd', ['/c', 'start', '', filePath]);
        return res.exitCode == 0;
      } else if (Platform.isLinux) {
        final res = await Process.run('xdg-open', [filePath]);
        return res.exitCode == 0;
      }
    } catch (_) {}
    return false;
  }

  /// 在系统文件管理器（Finder / Explorer）中定位并高亮显示文件
  static Future<bool> revealInFolder(String filePath) async {
    try {
      final file = File(filePath);
      if (!file.existsSync()) return false;

      if (Platform.isMacOS) {
        final res = await Process.run('open', ['-R', filePath]);
        return res.exitCode == 0;
      } else if (Platform.isWindows) {
        final res = await Process.run('explorer', ['/select,', filePath]);
        return res.exitCode == 0;
      } else if (Platform.isLinux) {
        final res = await Process.run('xdg-open', [file.parent.path]);
        return res.exitCode == 0;
      }
    } catch (_) {}
    return false;
  }

  /// 打开指定目录
  static Future<bool> openDirectory(String dirPath) async {
    try {
      final dir = Directory(dirPath);
      if (!dir.existsSync()) {
        dir.createSync(recursive: true);
      }
      if (Platform.isMacOS) {
        final res = await Process.run('open', [dirPath]);
        return res.exitCode == 0;
      } else if (Platform.isWindows) {
        final res = await Process.run('explorer', [dirPath]);
        return res.exitCode == 0;
      } else if (Platform.isLinux) {
        final res = await Process.run('xdg-open', [dirPath]);
        return res.exitCode == 0;
      }
    } catch (_) {}
    return false;
  }
}
