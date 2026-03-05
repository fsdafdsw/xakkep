import SwiftUI

struct MeowCounterView: View {
    let count: Int
    let bump: Bool

    var body: some View {
        HStack(spacing: 12) {
            Text("мяу")
                .font(.system(size: 15, weight: .semibold, design: .rounded))
                .foregroundStyle(Color.black.opacity(0.55))

            Text("\(count)")
                .font(.system(size: 22, weight: .semibold, design: .rounded))
                .foregroundStyle(Color.black.opacity(0.85))
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 10)
        .background(.ultraThinMaterial)
        .clipShape(Capsule())
        .overlay(
            Capsule().stroke(Color.white.opacity(0.6), lineWidth: 1)
        )
        .scaleEffect(bump ? 1.06 : 1.0)
        .animation(.spring(response: 0.3, dampingFraction: 0.6), value: bump)
        .shadow(color: Color.black.opacity(0.15), radius: 14, x: 0, y: 8)
    }
}

#Preview {
    MeowCounterView(count: 12, bump: true)
        .padding()
}
