#!/bin/bash
# ============================================================================
# Agency Outreach Bot — One-Shot Google Cloud Deploy Script
# ============================================================================
# Run this once. It handles EVERYTHING:
#   1. gcloud PATH fix
#   2. Authentication check
#   3. Project creation
#   4. Billing setup
#   5. API enablement
#   6. Full deployment
#   7. Prints your live URL
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo ""
echo -e "${PURPLE}============================================${NC}"
echo -e "${PURPLE}  Agency Outreach Bot — Cloud Deploy${NC}"
echo -e "${PURPLE}============================================${NC}"
echo ""

# ----------------------------------------------------------------------------
# STEP 1: Fix gcloud PATH
# ----------------------------------------------------------------------------
echo -e "${BLUE}[1/7] Setting up gcloud...${NC}"

# Try common gcloud locations
if ! command -v gcloud &> /dev/null; then
    GCLOUD_PATHS=(
        "$HOME/google-cloud-sdk/bin"
        "$HOME/gcloud init/google-cloud-sdk/bin"
        "/usr/local/google-cloud-sdk/bin"
        "/usr/local/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/bin"
        "/opt/homebrew/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/bin"
    )
    for p in "${GCLOUD_PATHS[@]}"; do
        if [ -f "$p/gcloud" ]; then
            export PATH="$p:$PATH"
            echo -e "${GREEN}  Found gcloud at: $p${NC}"
            break
        fi
    done
fi

if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}  ERROR: gcloud not found anywhere. Install it first:${NC}"
    echo -e "${RED}  https://cloud.google.com/sdk/docs/install${NC}"
    exit 1
fi

GCLOUD_VERSION=$(gcloud --version 2>/dev/null | head -1)
echo -e "${GREEN}  $GCLOUD_VERSION${NC}"

# ----------------------------------------------------------------------------
# STEP 2: Check authentication
# ----------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[2/7] Checking authentication...${NC}"

ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null)
if [ -z "$ACCOUNT" ]; then
    echo -e "${YELLOW}  Not logged in. Opening browser to sign in...${NC}"
    gcloud auth login
    ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null)
fi
echo -e "${GREEN}  Logged in as: $ACCOUNT${NC}"

# ----------------------------------------------------------------------------
# STEP 3: Create or select project
# ----------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[3/7] Setting up Google Cloud project...${NC}"

PROJECT_ID="rezthegiant-agency"

# Check if project exists
if gcloud projects describe "$PROJECT_ID" &> /dev/null; then
    echo -e "${GREEN}  Project '$PROJECT_ID' exists.${NC}"
else
    echo -e "${YELLOW}  Creating project '$PROJECT_ID'...${NC}"
    if ! gcloud projects create "$PROJECT_ID" --name="Agency Outreach Bot" 2>/dev/null; then
        # Project ID might be taken globally, try with random suffix
        SUFFIX=$(date +%s | tail -c 5)
        PROJECT_ID="rezthegiant-bot-${SUFFIX}"
        echo -e "${YELLOW}  Name taken. Trying '$PROJECT_ID'...${NC}"
        gcloud projects create "$PROJECT_ID" --name="Agency Outreach Bot"
    fi
    echo -e "${GREEN}  Project created: $PROJECT_ID${NC}"
fi

gcloud config set project "$PROJECT_ID" 2>/dev/null
echo -e "${GREEN}  Active project: $PROJECT_ID${NC}"

# ----------------------------------------------------------------------------
# STEP 4: Check and setup billing
# ----------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[4/7] Checking billing...${NC}"

BILLING_ACCOUNT=$(gcloud billing accounts list --format="value(ACCOUNT_ID)" 2>/dev/null | head -1)

if [ -z "$BILLING_ACCOUNT" ]; then
    echo ""
    echo -e "${YELLOW}  ⚠️  No billing account found.${NC}"
    echo -e "${YELLOW}  You need to set up billing (it's free — Cloud Run has a generous free tier).${NC}"
    echo ""
    echo -e "${YELLOW}  Opening Google Cloud billing setup in your browser...${NC}"
    open "https://console.cloud.google.com/billing/create?project=$PROJECT_ID" 2>/dev/null || \
        echo -e "${YELLOW}  Go to: https://console.cloud.google.com/billing/create?project=$PROJECT_ID${NC}"
    echo ""
    echo -e "${YELLOW}  After you set up billing, press ENTER to continue...${NC}"
    read -r
    BILLING_ACCOUNT=$(gcloud billing accounts list --format="value(ACCOUNT_ID)" 2>/dev/null | head -1)
fi

if [ -n "$BILLING_ACCOUNT" ]; then
    # Link billing to project
    LINKED=$(gcloud billing projects describe "$PROJECT_ID" --format="value(billingEnabled)" 2>/dev/null)
    if [ "$LINKED" != "True" ]; then
        echo -e "${YELLOW}  Linking billing account to project...${NC}"
        gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT" 2>/dev/null || true
    fi
    echo -e "${GREEN}  Billing is active.${NC}"
else
    echo -e "${RED}  ERROR: Billing is required. Set it up at:${NC}"
    echo -e "${RED}  https://console.cloud.google.com/billing${NC}"
    echo -e "${RED}  Then run this script again.${NC}"
    exit 1
fi

# ----------------------------------------------------------------------------
# STEP 5: Enable required APIs
# ----------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[5/7] Enabling Cloud Run APIs (this takes ~60 seconds)...${NC}"

gcloud services enable run.googleapis.com 2>/dev/null
echo -e "${GREEN}  ✓ Cloud Run API${NC}"
gcloud services enable cloudbuild.googleapis.com 2>/dev/null
echo -e "${GREEN}  ✓ Cloud Build API${NC}"
gcloud services enable artifactregistry.googleapis.com 2>/dev/null
echo -e "${GREEN}  ✓ Artifact Registry API${NC}"

# ----------------------------------------------------------------------------
# STEP 6: Get SMTP password and deploy
# ----------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[6/7] Deploying to Cloud Run...${NC}"
echo ""

# Ask for App Password if not set
if [ -z "$SMTP_PASS" ]; then
    echo -e "${YELLOW}  Enter your Google Workspace App Password for hello@rezthegiant.com${NC}"
    echo -e "${YELLOW}  (the 16-character password, no spaces — e.g. abcdefghijklmnop):${NC}"
    echo ""
    read -r -p "  App Password: " SMTP_PASS
    echo ""
fi

if [ -z "$SMTP_PASS" ]; then
    echo -e "${RED}  ERROR: App Password is required to send emails.${NC}"
    exit 1
fi

# Make sure we're in the server directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${YELLOW}  Building and deploying (this takes 2-3 minutes)...${NC}"
echo ""

gcloud run deploy agency-outreach-bot \
    --source . \
    --region us-central1 \
    --allow-unauthenticated \
    --set-env-vars="SMTP_USER=hello@rezthegiant.com,SMTP_FROM=hello@rezthegiant.com,SMTP_HOST=smtp.gmail.com,SMTP_PORT=587,SMTP_PASS=$SMTP_PASS" \
    --quiet

# ----------------------------------------------------------------------------
# STEP 7: Get the URL and celebrate
# ----------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[7/7] Getting your live URL...${NC}"

SERVICE_URL=$(gcloud run services describe agency-outreach-bot --region us-central1 --format="value(status.url)" 2>/dev/null)

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  🚀 DEPLOYMENT COMPLETE!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "${GREEN}  Your dashboard is live at:${NC}"
echo -e "${PURPLE}  $SERVICE_URL${NC}"
echo ""
echo -e "${GREEN}  To connect rezthegiant.com later:${NC}"
echo -e "${GREEN}  1. Go to https://console.cloud.google.com/run${NC}"
echo -e "${GREEN}  2. Click agency-outreach-bot → Custom Domains${NC}"
echo -e "${GREEN}  3. Add outreach.rezthegiant.com${NC}"
echo -e "${GREEN}  4. Add the CNAME record to Google Domains${NC}"
echo ""
echo -e "${GREEN}  To redeploy after changes:${NC}"
echo -e "${GREEN}  Just run this script again!${NC}"
echo ""

# Open in browser
open "$SERVICE_URL" 2>/dev/null || echo -e "  Open the URL above in your browser."
