import 'package:flutter/material.dart';
import 'ui/home_screen.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const ConnectionsApp());
}

class ConnectionsApp extends StatelessWidget {
  const ConnectionsApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: '邻传',
      debugShowCheckedModeBanner: false,
      themeMode: ThemeMode.dark,
      darkTheme: ThemeData(
        useMaterial3: true,
        brightness: Brightness.dark,
        scaffoldBackgroundColor: const Color(0xFF09090B),
        colorScheme: const ColorScheme.dark(
          surface: Color(0xFF121215),
          primary: Color(0xFF3B82F6),
          onPrimary: Colors.white,
          secondary: Color(0xFF27272A),
          onSurface: Color(0xFFF4F4F5),
          outline: Color(0xFF27272A),
        ),
        dividerColor: const Color(0xFF27272A),
        snackBarTheme: SnackBarThemeData(
          backgroundColor: const Color(0xFF18181C),
          contentTextStyle: const TextStyle(
            color: Color(0xFFF4F4F5),
            fontSize: 13,
          ),
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(8),
            side: const BorderSide(color: Color(0xFF27272A)),
          ),
        ),
      ),
      home: const HomeScreen(),
    );
  }
}
