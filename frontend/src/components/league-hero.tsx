import { useEffect, useState } from "react"
import { useNavigate } from "react-router-dom"
import axios from "axios"

interface LeaderboardPlayer {
  position: number
  osu_id: number
  username: string
  avatar_url: string | null
  score: number
}

axios.defaults.withCredentials = true

export function LeagueHero() {
  const navigate = useNavigate()
  const [topPlayers, setTopPlayers] = useState<LeaderboardPlayer[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchLeaderboard()
  }, [])

  const fetchLeaderboard = async () => {
    try {
      const response = await axios.get("http://localhost:8000/api/leaderboard")
      const leaderboard = response.data.leaderboard || []
      // Get top 3 for podium display
      setTopPlayers(leaderboard.slice(0, 3))
      setLoading(false)
    } catch (error) {
      console.error("Error fetching leaderboard:", error)
      setLoading(false)
    }
  }

  if (loading) {
    return (
      <section className="relative overflow-hidden bg-gradient-to-b from-[#2c3e50] to-[#34495e] py-16">
        <div className="container mx-auto px-6">
          <p className="text-center text-slate-400">Loading leaderboard...</p>
        </div>
      </section>
    )
  }

  return (
    <section className="relative overflow-hidden bg-gradient-to-b from-[#2c3e50] to-[#34495e] py-16">
      {/* My Team Button */}
      <div className="absolute top-4 left-4">
        <button
          onClick={() => navigate('/league/owc2025/my-team')}
          className="button-texture bg-[#f39c12] hover:bg-[#e67e22] text-white font-semibold px-6 py-2 rounded-lg transition-colors"
        >
          My Team
        </button>
      </div>

      <div className="container mx-auto px-6">
        <h2 className="mb-2 text-center text-5xl font-bold text-[#f39c12]">OWC 2025 Fantasy League</h2>
        <p className="mb-12 text-center text-lg text-slate-400">Top Players Leaderboard</p>

        {topPlayers.length === 0 ? (
          <p className="text-center text-slate-400">No players yet. Be the first to join!</p>
        ) : (
          <div className="mx-auto flex max-w-5xl items-end justify-center gap-8">
            {topPlayers.map((player) => (
              <div
                key={player.position}
                className={`flex flex-col items-center ${player.position === 1 ? "order-2" : player.position === 2 ? "order-1" : "order-3"}`}
              >
                <div
                  className={`relative mb-4 flex items-center justify-center rounded-lg bg-gradient-to-b from-slate-600 to-slate-700 shadow-xl overflow-hidden ${
                    player.position === 1
                      ? "h-56 w-56 border-4 border-dashed border-[#f39c12]"
                      : "h-44 w-44 border-2 border-slate-600"
                  }`}
                >
                  {player.avatar_url ? (
                    <img 
                      src={player.avatar_url} 
                      alt={player.username}
                      className="w-full h-full object-cover"
                    />
                  ) : (
                    <div className={`flex flex-col items-center ${player.position === 1 ? "scale-125" : ""}`}>
                      <span className={`text-7xl font-bold ${player.position === 1 ? "text-[#f39c12]" : "text-slate-400"}`}>
                        {player.position}
                      </span>
                    </div>
                  )}
                  <div className="absolute bottom-0 left-0 right-0 bg-black/60 py-1 text-center">
                    <span className={`text-2xl font-bold ${player.position === 1 ? "text-[#f39c12]" : "text-slate-300"}`}>
                      #{player.position}
                    </span>
                  </div>
                </div>
                <p
                  className={`mb-1 text-lg font-semibold ${player.position === 1 ? "text-[#f39c12]" : "text-slate-300"}`}
                >
                  {player.username}
                </p>
                <p className="text-sm text-slate-400">{player.score} points</p>
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  )
}
