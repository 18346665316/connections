import 'package:flutter/foundation.dart';

class TransferLogger {
  static String _nowTime() {
    final now = DateTime.now();
    final h = now.hour.toString().padLeft(2, '0');
    final m = now.minute.toString().padLeft(2, '0');
    final s = now.second.toString().padLeft(2, '0');
    final ms = now.millisecond.toString().padLeft(3, '0');
    return '$h:$m:$s.$ms';
  }

  static void log(String tag, String message) {
    debugPrint('[${_nowTime()}][$tag] $message');
  }

  static void discovery(String message) => log('Discovery', message);
  static void send(String message) => log('Send', message);
  static void recv(String message) => log('Recv', message);
  static void error(String tag, String message) => log('$tag:ERROR', message);
}
