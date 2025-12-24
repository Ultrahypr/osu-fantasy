import { ChevronRight } from "lucide-react"
import { useNavigate } from "react-router-dom"

interface LeagueItemProps {
  leagueName: string
  artist?: string
  author?: string
  timestamp?: string
  thumbnailUrl?: string
  onClick?: () => void
}

export function LeagueItem({
  leagueName,
  author = "osu!",
  timestamp = "Live NOW!!",
  thumbnailUrl = "../assets/owc2025-banner.jpg",
  onClick,
}: LeagueItemProps) {
  const navigate = useNavigate()
  
  const handleClick = () => {
    if (onClick) {
      onClick()
    } else {
      // Default behavior: navigate to league detail page
      navigate('/league/owc2025')
    }
  }

  return (
    <button
      onClick={handleClick}
      className="button-texture group flex w-full items-center gap-[50px] bg-[#2a2a3e] p-4 transition-colors hover:bg-[#32324a] active:bg-[#3a3a52]"
    >
      {/* Thumbnail */}
      <div className="relative w-[150px] h-[120px] shrink-0 overflow-hidden rounded">
        <img src={thumbnailUrl || "/placeholder.svg"} alt={leagueName} className="size-full object-cover" />
      </div>

      {/* Content */}
      <div className="flex min-w-0 flex-1 flex-col items-start gap-1 text-left">
        <h3 className="text-2xl font-semibold text-white">{leagueName}</h3>

        <p className="text-base text-gray-400">
          <span className="text-gray-500">{timestamp}</span>
        </p>
      </div>

      {/* Chevron */}
      <ChevronRight className="size-5 shrink-0 text-gray-400 transition-transform group-hover:translate-x-1" />
    </button>
  )
}
