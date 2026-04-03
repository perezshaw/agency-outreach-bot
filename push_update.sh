#!/bin/bash
# Push the full update: 1006 brands, Excel export, bulk send, and all improvements
cd "$(dirname "$0")"

# Add the new files and changes
git add app.py static/index.html brands_seed.json push_update.sh

git commit -m "Major update: 1006 brands, Excel export, bulk email campaigns

- Load 1006 brands from brands_seed.json on first startup
- Built-in XLSX generator (Python stdlib only) for master Excel export
- Two-tab Excel: Brands tab (all 1006) + Email Log tab (all outreach history)
- Bulk email endpoint: select brands by vertical, send personalized templates
- Frontend: Export Excel button, Bulk Send modal with vertical filter
- Brand count badge shows total brands in database
- Email templates: Cold Intro, Follow-Up, Rate Card with auto-fill variables"

git push
echo ""
echo "✅ Pushed! Render will auto-deploy in ~2 minutes."
echo "   Live at: https://agency-outreach-bot.onrender.com"
echo ""
echo "Next: Set up outreach.rezthegiant.com (see instructions below)"
echo "   1. Go to dashboard.render.com → your service → Settings → Custom Domains"
echo "   2. Add: outreach.rezthegiant.com"
echo "   3. Render will give you a CNAME target (something like xxx.onrender.com)"
echo "   4. In your DNS provider, add CNAME record:"
echo "      Name: outreach    Value: [the target Render gives you]"
