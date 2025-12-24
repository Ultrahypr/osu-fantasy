import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

interface Player {
  id: number;
  username: string;
  profile_url: string;
  avatar_url: string;
  country: string;
  cost: number;
}

interface TeamData {
  player_ids: number[];
  budget_used: number;
  budget_remaining: number;
}

const API_BASE = 'http://localhost:8000';

export default function MyTeamPage() {
  const navigate = useNavigate();
  const [players, setPlayers] = useState<Player[]>([]);
  const [selectedIds, setSelectedIds] = useState<number[]>([]);
  const [totalBudget, setTotalBudget] = useState(100);
  const [maxTeamSize, setMaxTeamSize] = useState(8);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [countryFilter, setCountryFilter] = useState('');

  // Calculate budget used
  const budgetUsed = selectedIds.reduce((sum, id) => {
    const player = players.find(p => p.id === id);
    return sum + (player?.cost || 0);
  }, 0);
  const budgetRemaining = totalBudget - budgetUsed;

  // Get unique countries for filter
  const countries = [...new Set(players.map(p => p.country))].sort();

  // Filter players
  const filteredPlayers = players.filter(player => {
    const matchesSearch = player.username.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesCountry = !countryFilter || player.country === countryFilter;
    return matchesSearch && matchesCountry;
  });

  useEffect(() => {
    fetchData();
  }, []);

  async function fetchData() {
    setLoading(true);
    setError(null);

    try {
      // Fetch players
      const playersRes = await fetch(`${API_BASE}/api/players?tournament=owc2025`);
      if (!playersRes.ok) throw new Error('Failed to fetch players');
      const playersData = await playersRes.json();
      setPlayers(playersData.players);
      setTotalBudget(playersData.total_budget);
      setMaxTeamSize(playersData.max_team_size);

      // Fetch current team (if authenticated)
      try {
        const teamRes = await fetch(`${API_BASE}/api/team?tournament=owc2025`, {
          credentials: 'include'
        });
        if (teamRes.ok) {
          const teamData: TeamData = await teamRes.json();
          setSelectedIds(teamData.player_ids || []);
        }
      } catch {
        // Not authenticated or no team yet - that's fine
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }

  function togglePlayer(playerId: number) {
    const player = players.find(p => p.id === playerId);
    if (!player) return;

    if (selectedIds.includes(playerId)) {
      // Remove player
      setSelectedIds(selectedIds.filter(id => id !== playerId));
    } else {
      // Add player (check budget and team size)
      if (selectedIds.length >= maxTeamSize) {
        setError(`Maximum team size is ${maxTeamSize} players`);
        return;
      }
      if (budgetUsed + player.cost > totalBudget) {
        setError('Not enough budget for this player');
        return;
      }
      setSelectedIds([...selectedIds, playerId]);
      setError(null);
    }
  }

  async function saveTeam() {
    setSaving(true);
    setError(null);

    try {
      const res = await fetch(`${API_BASE}/api/team`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          player_ids: selectedIds,
          tournament: 'owc2025'
        })
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.detail || 'Failed to save team');
      }

      alert('Team saved successfully!');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save team');
    } finally {
      setSaving(false);
    }
  }

  const selectedPlayers = players.filter(p => selectedIds.includes(p.id));

  if (loading) {
    return (
      <div className="min-h-screen bg-[#1a1a2e] text-white flex items-center justify-center">
        <div className="text-xl">Loading players...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#1a1a2e] text-white">
      {/* Header */}
      <div className="bg-[#16162a] border-b border-white/10">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center justify-between">
          <button
            onClick={() => navigate('/league/owc2025')}
            className="button-texture bg-[#2a2a4e] hover:bg-[#3a3a5e] px-4 py-2 rounded-lg flex items-center gap-2"
          >
            <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            </svg>
            Back
          </button>
          <h1 className="text-2xl font-bold">My Team - OWC 2025</h1>
          <button
            onClick={saveTeam}
            disabled={saving || selectedIds.length === 0}
            className="button-texture bg-[#f39c12] hover:bg-[#e67e22] disabled:bg-gray-600 disabled:cursor-not-allowed px-6 py-2 rounded-lg font-semibold"
          >
            {saving ? 'Saving...' : 'Save Team'}
          </button>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6">
        {/* Budget Bar */}
        <div className="bg-[#2a2a4e] rounded-xl p-4 mb-6">
          <div className="flex justify-between items-center mb-2">
            <span className="text-lg font-semibold">Budget</span>
            <span className={`text-lg font-bold ${budgetRemaining < 10 ? 'text-red-400' : 'text-green-400'}`}>
              {budgetRemaining} / {totalBudget} remaining
            </span>
          </div>
          <div className="w-full bg-[#1a1a2e] rounded-full h-4 overflow-hidden">
            <div
              className={`h-full transition-all duration-300 ${budgetRemaining < 10 ? 'bg-red-500' : 'bg-green-500'}`}
              style={{ width: `${(budgetRemaining / totalBudget) * 100}%` }}
            />
          </div>
          <div className="mt-2 text-sm text-gray-400">
            Team: {selectedIds.length} / {maxTeamSize} players
          </div>
        </div>

        {error && (
          <div className="bg-red-500/20 border border-red-500 text-red-300 px-4 py-3 rounded-lg mb-6">
            {error}
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Selected Team */}
          <div className="lg:col-span-1">
            <div className="bg-[#2a2a4e] rounded-xl p-4 sticky top-4">
              <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
                <svg className="text-[#f39c12]" style={{ width: 20, height: 20 }} fill="currentColor" viewBox="0 0 20 20">
                  <path d="M13 6a3 3 0 11-6 0 3 3 0 016 0zM18 8a2 2 0 11-4 0 2 2 0 014 0zM14 15a4 4 0 00-8 0v3h8v-3zM6 8a2 2 0 11-4 0 2 2 0 014 0zM16 18v-3a5.972 5.972 0 00-.75-2.906A3.005 3.005 0 0119 15v3h-3zM4.75 12.094A5.973 5.973 0 004 15v3H1v-3a3 3 0 013.75-2.906z" />
                </svg>
                Your Team ({selectedIds.length}/{maxTeamSize})
              </h2>

              {selectedPlayers.length === 0 ? (
                <div className="text-gray-400 text-center py-8">
                  Click on players to add them to your team
                </div>
              ) : (
                <div className="space-y-2">
                  {selectedPlayers.map(player => (
                    <div
                      key={player.id}
                      className="flex items-center gap-3 bg-[#1a1a2e] rounded-lg p-3 group"
                    >
                      <button
                        type="button"
                        onClick={() => togglePlayer(player.id)}
                        className="shrink-0 rounded-full focus:outline-none focus:ring-2 focus:ring-[#f39c12] focus:ring-offset-2 focus:ring-offset-[#1a1a2e]"
                        title={`Remove ${player.username}`}
                      >
                        <img
                          src={player.avatar_url || `https://a.ppy.sh/${player.id}`}
                          alt={player.username}                          className="rounded-full object-cover"
                          style={{ width: 60, height: 60 }}
                          onError={(e) => {
                            (e.target as HTMLImageElement).src = 'https://osu.ppy.sh/images/layout/avatar-guest.png';
                          }}
                        />
                      </button>
                      <div className="flex-1 min-w-0">
                        <div className="font-medium truncate">{player.username}</div>
                        <div className="text-sm text-gray-400">{player.country}</div>
                      </div>
                      <div className="text-[#f39c12] font-bold">${player.cost}</div>
                      <button
                        onClick={() => togglePlayer(player.id)}
                        className="opacity-0 group-hover:opacity-100 transition-opacity text-red-400 hover:text-red-300"
                      >
                        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  ))}
                </div>
              )}

              <div className="mt-4 pt-4 border-t border-white/10">
                <div className="flex justify-between text-lg">
                  <span>Total Cost:</span>
                  <span className="font-bold text-[#f39c12]">${budgetUsed}</span>
                </div>
              </div>
            </div>
          </div>

          {/* Player Selection */}
          <div className="lg:col-span-2">
            <div className="bg-[#2a2a4e] rounded-xl p-4">
              <h2 className="text-xl font-bold mb-4">Available Players ({filteredPlayers.length})</h2>

              {/* Filters */}
              <div className="flex gap-4 mb-4">
                <input
                  type="text"
                  placeholder="Search players..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="flex-1 bg-[#1a1a2e] border border-white/10 rounded-lg px-4 py-2 focus:outline-none focus:border-[#f39c12]"
                />
                <select
                  value={countryFilter}
                  onChange={(e) => setCountryFilter(e.target.value)}
                  className="bg-[#1a1a2e] border border-white/10 rounded-lg px-4 py-2 focus:outline-none focus:border-[#f39c12]"
                >
                  <option value="">All Countries</option>
                  {countries.map(country => (
                    <option key={country} value={country}>{country}</option>
                  ))}
                </select>
              </div>

              {/* Player Grid */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 max-h-[600px] overflow-y-auto pr-2">
                {filteredPlayers.map(player => {
                  const isSelected = selectedIds.includes(player.id);
                  const canAfford = budgetRemaining >= player.cost;
                  const teamFull = selectedIds.length >= maxTeamSize;

                  return (
                    <button
                      key={player.id}
                      onClick={() => togglePlayer(player.id)}
                      disabled={!isSelected && (!canAfford || teamFull)}
                      className={`
                        button-texture flex items-center gap-3 p-3 rounded-lg text-left transition-all
                        ${isSelected
                          ? 'bg-[#f39c12]/20 border-2 border-[#f39c12]'
                          : canAfford && !teamFull
                            ? 'bg-[#1a1a2e] hover:bg-[#252545] border-2 border-transparent'
                            : 'bg-[#1a1a2e] opacity-50 cursor-not-allowed border-2 border-transparent'
                        }
                      `}
                    >
                      <img
                        src={player.avatar_url || `https://a.ppy.sh/${player.id}`}
                        alt={player.username}
                        className="rounded-full object-cover"
                        style={{ width: 40, height: 40 }}
                        onError={(e) => {
                          (e.target as HTMLImageElement).src = 'https://osu.ppy.sh/images/layout/avatar-guest.png';
                        }}
                      />
                      <div className="flex-1 min-w-0">
                        <div className="font-medium truncate">{player.username}</div>
                        <div className="text-sm text-gray-400">{player.country}</div>
                      </div>
                      <div className={`font-bold ${isSelected ? 'text-[#f39c12]' : 'text-green-400'}`}>
                        ${player.cost}
                      </div>
                      {isSelected && (
                        <svg className="text-[#f39c12]" style={{ width: 12, height: 12 }} fill="currentColor" viewBox="0 0 20 20">
                          <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                        </svg>
                      )}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
