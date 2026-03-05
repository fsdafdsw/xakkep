# iOS версия (SwiftUI)

Ниже — быстрый путь, чтобы запустить нативное iOS-приложение на iPad. Все работает офлайн.

## 1) Установка Xcode (один раз)
1. Открой App Store на Mac.
2. Найди Xcode и установи (10–12 ГБ).
3. Запусти Xcode и прими лицензию.

Проверка: в Terminal запусти `xcode-select -p`. Должно быть что-то вроде `/Applications/Xcode.app/Contents/Developer`.

## 2) Создай новый проект в Xcode
1. File -> New -> Project.
2. iOS -> App.
3. Product Name: `MeowButton`.
4. Interface: `SwiftUI`.
5. Language: `Swift`.
6. Organization Identifier: `com.yourname`.
7. Bundle Identifier сформируется автоматически.
8. Сохрани проект в папку `ios/MeowButtonApp` рядом с этим README.

## 3) Подмени файлы кода
Из папки `ios/MeowButton/` возьми и замени/добавь файлы в Xcode:
- `ContentView.swift`
- `MeowAudioPlayer.swift` (добавь как новый файл)
- `MeowButtonApp.swift` (замени созданный App файл)

## 4) Добавь ассеты
Добавь в Xcode (drag & drop) файлы из корневой папки проекта:
- `assets/cat.jpg` -> в Assets.xcassets как `cat`
- `assets/meow.mp3` (или `assets/meow.wav`) -> в target (обычный файл, Copy items if needed)

Имена должны быть именно `cat` и `meow.mp3`.

## 5) Запуск на iPad
1. Подключи iPad по кабелю или включи Wi‑Fi debugging.
2. В Xcode выбери устройство iPad в верхней панели.
3. Нажми Play.
4. При первом запуске на iPad: Settings -> General -> VPN & Device Management -> Trust.

Готово.
