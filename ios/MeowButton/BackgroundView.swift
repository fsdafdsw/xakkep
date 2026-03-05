import SwiftUI

struct BackgroundView: View {
    var body: some View {
        ZStack {
            LinearGradient(
                colors: [Color.white, Color(red: 0.95, green: 0.96, blue: 0.98)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            Circle()
                .fill(
                    RadialGradient(
                        colors: [Color.white.opacity(0.9), Color.blue.opacity(0.08), .clear],
                        center: .topLeading,
                        startRadius: 0,
                        endRadius: 420
                    )
                )
                .frame(width: 520, height: 520)
                .offset(x: -160, y: -240)
                .blur(radius: 30)

            Circle()
                .fill(
                    RadialGradient(
                        colors: [Color.blue.opacity(0.12), Color.gray.opacity(0.05), .clear],
                        center: .bottomTrailing,
                        startRadius: 0,
                        endRadius: 420
                    )
                )
                .frame(width: 520, height: 520)
                .offset(x: 160, y: 260)
                .blur(radius: 30)
        }
    }
}

#Preview {
    BackgroundView()
}
