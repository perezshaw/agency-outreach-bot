#!/bin/bash
###############################################################################
#  Agency Outreach Bot — One-Shot Render.com Deployment
#
#  This script does EVERYTHING:
#    1. Initializes a git repo in /server
#    2. Creates a GitHub repo (installs GitHub CLI if needed)
#    3. Pushes your code
#    4. Deploys to Render.com via their API
#    5. Sets SMTP environment variables
#    6. Prints your live URL
#
#  Run from: ~/Documents/Claude/Projects/AgencyOutreachBot/server/
#  Usage:    chmod +x deploy_render.sh && ./deploy_render.sh
###############################################################################

set -e

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║   Agency Outreach Bot — Render.com Deployment Script    ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# ─── Step 1: Check we're in the right directory ──────────────────────────────
if [ ! -f "app.py" ] || [ ! -d "static" ]; then
    echo -e "${RED}ERROR: Run this script from the server/ directory${NC}"
    echo "  cd ~/Documents/Claude/Projects/AgencyOutreachBot/server"
    echo "  ./deploy_render.sh"
    exit 1
fi

echo -e "${GREEN}✓ In server/ directory${NC}"

# ─── Step 2: Install GitHub CLI if missing ───────────────────────────────────
if ! command -v gh &> /dev/null; then
    echo -e "${YELLOW}→ GitHub CLI (gh) not found. Installing via Homebrew...${NC}"
    if ! command -v brew &> /dev/null; then
        echo -e "${YELLOW}→ Homebrew not found. Installing Homebrew first...${NC}"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        # Add brew to path for Apple Silicon
        if [ -f "/opt/homebrew/bin/brew" ]; then
            eval "$(/opt/homebrew/bin/brew shellenv)"
        fi
    fi
    brew install gh
    echo -e "${GREEN}✓ GitHub CLI installed${NC}"
else
    echo -e "${GREEN}✓ GitHub CLI already installed${NC}"
fi

# ─── Step 3: Authenticate GitHub CLI ─────────────────────────────────────────
if ! gh auth status &> /dev/null 2>&1; then
    echo -e "${YELLOW}→ You need to log into GitHub. A browser window will open.${NC}"
    gh auth login --web --git-protocol https
fi
echo -e "${GREEN}✓ GitHub authenticated${NC}"

# ─── Step 4: Initialize git repo ─────────────────────────────────────────────
if [ ! -d ".git" ]; then
    git init
    echo -e "${GREEN}✓ Git repo initialized${NC}"
else
    echo -e "${GREEN}✓ Git repo already exists${NC}"
fi

# Create .gitignore
cat > .gitignore << 'GITIGNORE'
__pycache__/
*.pyc
*.pyo
*.db
*.sqlite3
.env
.DS_Store
*.md
GITIGNORE

# Stage and commit
git add -A
git commit -m "Agency Outreach Bot - initial deployment" 2>/dev/null || echo -e "${GREEN}✓ No new changes to commit${NC}"

# ─── Step 5: Create GitHub repo and push ─────────────────────────────────────
REPO_NAME="agency-outreach-bot"

# Check if repo already exists on GitHub
if gh repo view "$(gh api user -q .login)/$REPO_NAME" &> /dev/null 2>&1; then
    echo -e "${GREEN}✓ GitHub repo already exists${NC}"
    GITHUB_USER=$(gh api user -q .login)
    git remote remove origin 2>/dev/null || true
    git remote add origin "https://github.com/$GITHUB_USER/$REPO_NAME.git"
else
    echo -e "${YELLOW}→ Creating GitHub repo: $REPO_NAME${NC}"
    gh repo create "$REPO_NAME" --private --source=. --remote=origin --push
    echo -e "${GREEN}✓ GitHub repo created and code pushed${NC}"
fi

# Push latest code
git push -u origin main 2>/dev/null || git push -u origin master 2>/dev/null || {
    BRANCH=$(git branch --show-current)
    git push -u origin "$BRANCH"
}
echo -e "${GREEN}✓ Code pushed to GitHub${NC}"

GITHUB_USER=$(gh api user -q .login)
REPO_URL="https://github.com/$GITHUB_USER/$REPO_NAME"

echo ""
echo -e "${CYAN}GitHub repo: $REPO_URL${NC}"
echo ""

# ─── Step 6: Deploy to Render ────────────────────────────────────────────────
echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Now let's deploy to Render.com${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Check for Render API key
if [ -z "$RENDER_API_KEY" ]; then
    echo -e "${YELLOW}To deploy automatically, you need a Render API key.${NC}"
    echo ""
    echo -e "  1. Go to ${CYAN}https://dashboard.render.com/settings#api-keys${NC}"
    echo -e "  2. Click ${CYAN}Create API Key${NC}"
    echo -e "  3. Copy the key"
    echo ""
    read -p "Paste your Render API key here (or press Enter to skip auto-deploy): " RENDER_API_KEY
    echo ""
fi

if [ -n "$RENDER_API_KEY" ]; then
    # ─── Automatic Render Deployment via API ──────────────────────────────
    echo -e "${YELLOW}→ Creating Render web service...${NC}"

    # Get SMTP password
    echo -e "${YELLOW}→ Email setup for outreach automation${NC}"
    echo -e "  Your bot sends emails from: ${CYAN}hello@rezthegiant.com${NC}"
    echo -e "  You need a Google App Password."
    echo -e "  (Get one at: ${CYAN}https://myaccount.google.com/apppasswords${NC})"
    echo ""
    read -sp "Enter your Google App Password (16 chars, no spaces): " APP_PASSWORD
    echo ""

    # Create the service via Render API
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "https://api.render.com/v1/services" \
        -H "Authorization: Bearer $RENDER_API_KEY" \
        -H "Content-Type: application/json" \
        -d "{
            \"type\": \"web_service\",
            \"name\": \"agency-outreach-bot\",
            \"repo\": \"$REPO_URL\",
            \"autoDeploy\": \"yes\",
            \"branch\": \"$(git branch --show-current)\",
            \"runtime\": \"python\",
            \"plan\": \"free\",
            \"startCommand\": \"python app.py\",
            \"envVars\": [
                {\"key\": \"PORT\", \"value\": \"10000\"},
                {\"key\": \"SMTP_HOST\", \"value\": \"smtp.gmail.com\"},
                {\"key\": \"SMTP_PORT\", \"value\": \"587\"},
                {\"key\": \"SMTP_USER\", \"value\": \"hello@rezthegiant.com\"},
                {\"key\": \"SMTP_PASS\", \"value\": \"$APP_PASSWORD\"},
                {\"key\": \"SMTP_FROM\", \"value\": \"hello@rezthegiant.com\"},
                {\"key\": \"DB_PATH\", \"value\": \"/tmp/agency_outreach.db\"},
                {\"key\": \"SETTINGS_PATH\", \"value\": \"/tmp/agency_settings.json\"}
            ]
        }")

    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    BODY=$(echo "$RESPONSE" | sed '$d')

    if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
        SERVICE_ID=$(echo "$BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service',{}).get('id',''))" 2>/dev/null || echo "")
        SERVICE_URL=$(echo "$BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service',{}).get('serviceDetails',{}).get('url',''))" 2>/dev/null || echo "")

        echo ""
        echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║              DEPLOYMENT SUCCESSFUL!                      ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
        echo ""
        if [ -n "$SERVICE_URL" ]; then
            echo -e "  Your dashboard is live at: ${CYAN}$SERVICE_URL${NC}"
        else
            echo -e "  Your app is deploying now. Check: ${CYAN}https://dashboard.render.com${NC}"
        fi
        echo -e "  GitHub repo: ${CYAN}$REPO_URL${NC}"
        echo ""
        echo -e "${YELLOW}  Note: First deploy takes 2-3 minutes. Render will show${NC}"
        echo -e "${YELLOW}  'Deploy in progress' until it's ready.${NC}"
        echo ""
        echo -e "${CYAN}  To connect rezthegiant.com:${NC}"
        echo -e "  1. Go to your service on Render dashboard"
        echo -e "  2. Click Settings → Custom Domains"
        echo -e "  3. Add: outreach.rezthegiant.com (or any subdomain)"
        echo -e "  4. Add the CNAME record Render gives you to your DNS"
        echo ""
    else
        echo -e "${RED}Render API returned status $HTTP_CODE${NC}"
        echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
        echo ""
        echo -e "${YELLOW}Falling back to manual deployment...${NC}"
        RENDER_API_KEY=""
    fi
fi

# ─── Fallback: Manual one-click deploy ───────────────────────────────────────
if [ -z "$RENDER_API_KEY" ]; then
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Manual Render Deployment (2 minutes)${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  Your code is on GitHub at: ${CYAN}$REPO_URL${NC}"
    echo ""
    echo -e "  ${YELLOW}Steps:${NC}"
    echo -e "  1. Go to ${CYAN}https://dashboard.render.com${NC}"
    echo -e "  2. Sign up / log in with your GitHub account"
    echo -e "  3. Click ${CYAN}New → Web Service${NC}"
    echo -e "  4. Connect your repo: ${CYAN}$REPO_NAME${NC}"
    echo -e "  5. Settings:"
    echo -e "       Name:          ${CYAN}agency-outreach-bot${NC}"
    echo -e "       Runtime:       ${CYAN}Python${NC}"
    echo -e "       Start Command: ${CYAN}python app.py${NC}"
    echo -e "       Plan:          ${CYAN}Free${NC}"
    echo -e "  6. Add these Environment Variables:"
    echo -e "       ${CYAN}PORT${NC}           = ${CYAN}10000${NC}"
    echo -e "       ${CYAN}SMTP_HOST${NC}      = ${CYAN}smtp.gmail.com${NC}"
    echo -e "       ${CYAN}SMTP_PORT${NC}      = ${CYAN}587${NC}"
    echo -e "       ${CYAN}SMTP_USER${NC}      = ${CYAN}hello@rezthegiant.com${NC}"
    echo -e "       ${CYAN}SMTP_PASS${NC}      = ${CYAN}(your Google App Password)${NC}"
    echo -e "       ${CYAN}SMTP_FROM${NC}      = ${CYAN}hello@rezthegiant.com${NC}"
    echo -e "       ${CYAN}DB_PATH${NC}        = ${CYAN}/tmp/agency_outreach.db${NC}"
    echo -e "       ${CYAN}SETTINGS_PATH${NC}  = ${CYAN}/tmp/agency_settings.json${NC}"
    echo -e "  7. Click ${CYAN}Deploy Web Service${NC}"
    echo ""
    echo -e "  ${YELLOW}To connect rezthegiant.com later:${NC}"
    echo -e "  → Render Settings → Custom Domains → Add your domain"
    echo -e "  → Add the CNAME record to your DNS provider"
    echo ""
fi

echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Done! Your Agency Outreach Bot deployment is complete.${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
