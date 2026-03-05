import SwiftUI

struct ContentView: View {
    @State private var meowCount = 0
    @State private var isBumping = false
    @State private var showPulse = false
    @State private var rippleID = UUID()
    @State private var lastTapTime: TimeInterval = 0
    @State private var streak = 0
    @State private var showStreak = false

    @StateObject private var audioPlayer = MeowAudioPlayer()

    var body: some View {
        ZStack {
            BackgroundView()

            VStack {
                HStack {
                    Spacer()
                    MeowCounter(count: meowCount, isBumping: isBumping)
                }
                .padding(.top, 24)
                .padding(.horizontal, 24)

                Spacer()

                ZStack {
                    CatButtonView(rippleID: rippleID)
                        .scaleEffect(buttonScale)
                        .onTapGesture {
                            handleTap()
                        }

                    if showPulse {
                        Text("мяу")
                            .font(.system(size: 20, weight: .semibold, design: .rounded))
                            .foregroundStyle(.secondary)
                            .tracking(6)
                            .transition(.asymmetric(
                                insertion: .move(edge: .bottom).combined(with: .opacity),
                                removal: .opacity
                            ))
                            .offset(y: -140)
                    }

                    if showStreak {
                        Text("x\(streak)")
                            .font(.system(size: 16, weight: .semibold, design: .rounded))
                            .padding(.vertical, 6)
                            .padding(.horizontal, 12)
                            .background(.ultraThinMaterial, in: Capsule())
                            .foregroundStyle(.secondary)
                            .offset(x: 110, y: -180)
                            .transition(.opacity)
                    }
                }

                Spacer()
            }
        }
        .ignoresSafeArea()
    }

    private var buttonScale: CGFloat {
        let boost = min(1.0 + CGFloat(streak) * 0.04, 1.35)
        return showStreak ? boost : 1.0
    }

    private func handleTap() {
        meowCount += 1
        audioPlayer.play()

        withAnimation(.spring(response: 0.35, dampingFraction: 0.6)) {
            isBumping = true
        }

        withAnimation(.easeOut(duration: 0.6)) {
            showPulse = true
        }

        withAnimation(.easeOut(duration: 0.7)) {
            rippleID = UUID()
        }

        updateStreak()

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.35) {
            withAnimation(.easeOut(duration: 0.3)) {
                isBumping = false
            }
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.6) {
            withAnimation(.easeIn(duration: 0.3)) {
                showPulse = false
            }
        }
    }

    private func updateStreak() {
        let now = Date().timeIntervalSince1970
        if now - lastTapTime < 0.9 {
            streak += 1
        } else {
            streak = 1
        }
        lastTapTime = now

        withAnimation(.easeOut(duration: 0.2)) {
            showStreak = true
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 1.1) {
            withAnimation(.easeIn(duration: 0.3)) {
                showStreak = false
                streak = 0
            }
        }
    }
}

struct MeowCounter: View {
    let count: Int
    let isBumping: Bool

    var body: some View {
        HStack(spacing: 10) {
            Text("мяу")
                .font(.system(size: 14, weight: .semibold, design: .rounded))
                .foregroundStyle(.secondary)
            Text("\(count)")
                .font(.system(size: 20, weight: .semibold, design: .rounded))
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 14)
        .background(.ultraThinMaterial, in: Capsule())
        .overlay(
            Capsule()
                .strokeBorder(Color.white.opacity(0.6), lineWidth: 1)
        )
        .shadow(color: .black.opacity(0.12), radius: 12, x: 0, y: 6)
        .scaleEffect(isBumping ? 1.06 : 1.0)
        .animation(.spring(response: 0.3, dampingFraction: 0.6), value: isBumping)
    }
}

struct CatButtonView: View {
    let rippleID: UUID

    var body: some View {
        ZStack {
            RoundedRectangle(cornerRadius: 36, style: .continuous)
                .fill(Color.white.opacity(0.65))
                .shadow(color: .black.opacity(0.16), radius: 30, x: 0, y: 12)
                .overlay(
                    RoundedRectangle(cornerRadius: 36, style: .continuous)
                        .stroke(Color.white.opacity(0.7), lineWidth: 1)
                )

            ZStack {
                Image("cat")
                    .resizable()
                    .scaledToFill()
                    .frame(width: 280, height: 360)
                    .clipped()
                    .cornerRadius(26)
                    .overlay(
                        RoundedRectangle(cornerRadius: 26, style: .continuous)
                            .fill(
                                LinearGradient(
                                    colors: [
                                        Color.white.opacity(0.35),
                                        Color.clear
                                    ],
                                    startPoint: .topLeading,
                                    endPoint: .bottomTrailing
                                )
                            )
                    )
            }
            .padding(12)

            Circle()
                .stroke(Color.white.opacity(0.8), lineWidth: 1)
                .frame(width: 20, height: 20)
                .scaleEffect(0.2)
                .opacity(0)
                .animation(.easeOut(duration: 0.7), value: rippleID)
                .id(rippleID)
        }
        .frame(width: 300, height: 420)
    }
}

struct BackgroundView: View {
    var body: some View {
        ZStack {
            LinearGradient(
                colors: [Color.white, Color(red: 0.94, green: 0.95, blue: 0.97)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )

            Circle()
                .fill(
                    RadialGradient(
                        colors: [Color.white.opacity(0.9), Color.blue.opacity(0.05)],
                        center: .center,
                        startRadius: 10,
                        endRadius: 240
                    )
                )
                .frame(width: 420, height: 420)
                .blur(radius: 40)
                .offset(x: -140, y: -260)
                .animation(.easeInOut(duration: 14).repeatForever(autoreverses: true), value: UUID())

            Circle()
                .fill(
                    RadialGradient(
                        colors: [Color.blue.opacity(0.22), Color.clear],
                        center: .center,
                        startRadius: 10,
                        endRadius: 260
                    )
                )
                .frame(width: 460, height: 460)
                .blur(radius: 40)
                .offset(x: 160, y: 240)
                .animation(.easeInOut(duration: 16).repeatForever(autoreverses: true), value: UUID())
        }
    }
}

#Preview {
    ContentView()
}
