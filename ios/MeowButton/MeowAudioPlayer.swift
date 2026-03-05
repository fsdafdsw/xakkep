import AVFoundation
import SwiftUI

final class MeowAudioPlayer: ObservableObject {
    private var player: AVAudioPlayer?

    init() {
        loadAudio()
    }

    func play() {
        if player == nil {
            loadAudio()
        }
        player?.currentTime = 0
        player?.play()
    }

    private func loadAudio() {
        let url =
            Bundle.main.url(forResource: "meow", withExtension: "mp3")
            ?? Bundle.main.url(forResource: "meow", withExtension: "wav")

        guard let url else { return }

        do {
            player = try AVAudioPlayer(contentsOf: url)
            player?.prepareToPlay()
        } catch {
            player = nil
        }
    }
}
