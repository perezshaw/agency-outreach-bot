#!/bin/bash
# Run this once to push the platform API integration to GitHub → auto-deploys to Render
cd "$(dirname "$0")"
git add app.py static/index.html
git commit -m "Integrate live platform APIs for real creator stats

- Add PlatformAPI class with YouTube, Twitch, Kick, Instagram, X, TikTok support
- SQLite caching layer (1-hour TTL) to avoid rate limit issues
- New endpoints: /api/stats/refresh, /api/stats/live/<id>, /api/ratecard/<id>, /api/platform_status
- Stats page: live data from APIs, Refresh Data button, last-updated timestamp
- Rate card generator: CPM-based pricing from real follower/engagement metrics
- Creator cards: Rate Card button linking directly to stats view
- Graceful fallback to mock data when API keys not yet configured"
git push
echo ""
echo "✅ Pushed! Render will auto-deploy in ~2 minutes."
echo "   Live at: https://agency-outreach-bot.onrender.com"
