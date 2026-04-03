#!/usr/bin/env python3
"""
Agency Outreach Bot - Production-Ready Backend Server
Runs on Python stdlib only (no external dependencies)
"""

import json
import os
import sqlite3
import smtplib
import re
import csv
import io
import threading
import time
import secrets
import urllib.request
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Any, Dict, List, Optional, Tuple
import uuid
from pathlib import Path

# Configuration
PORT = int(os.environ.get('PORT', 5000))
SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
SMTP_USER = os.environ.get('SMTP_USER', '')
SMTP_PASS = os.environ.get('SMTP_PASS', '')
SMTP_FROM = os.environ.get('SMTP_FROM', '')
DB_PATH = os.environ.get('DB_PATH', '/tmp/agency_outreach.db')
SETTINGS_PATH = os.environ.get('SETTINGS_PATH', '/tmp/agency_settings.json')
BASE_DIR = Path(__file__).parent
APP_PASSWORD = os.environ.get('APP_PASSWORD', '')  # Empty = no auth required

# Platform API keys (set as environment variables on Render)
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY', '')
TWITCH_CLIENT_ID = os.environ.get('TWITCH_CLIENT_ID', '')
TWITCH_CLIENT_SECRET = os.environ.get('TWITCH_CLIENT_SECRET', '')
X_BEARER_TOKEN = os.environ.get('X_BEARER_TOKEN', '')
INSTAGRAM_ACCESS_TOKEN = os.environ.get('INSTAGRAM_ACCESS_TOKEN', '')
TIKTOK_ACCESS_TOKEN = os.environ.get('TIKTOK_ACCESS_TOKEN', '')

# Session store (in-memory)
VALID_SESSIONS = set()

# Creator Data (embedded)
CREATORS = {
    "1": {
        "id": "1",
        "name": "Kalani Rodgers",
        "niche": "Comedy",
        "bio": "Stand-up comedian and comedy content creator with a focus on observational humor and social commentary.",
        "platforms": {
            "tiktok": {"handle": "@kalanirodgers", "followers": 450000, "engagement_rate": 8.2},
            "instagram": {"handle": "@kalanirodgers", "followers": 320000, "engagement_rate": 7.5},
            "youtube": {"handle": "Kalani Rodgers", "followers": 180000, "engagement_rate": 6.8},
            "twitter": {"handle": "@kalanirodgers", "followers": 95000, "engagement_rate": 5.4}
        },
        "total_reach": 1045000,
        "avg_engagement_rate": 7.0,
        "verticals": ["Comedy", "Entertainment", "Lifestyle"],
        "services": [
            {"name": "TikTok/Instagram Reels", "rate": 2000, "per": "post"},
            {"name": "YouTube Video", "rate": 5000, "per": "video"},
            {"name": "Brand Collaboration", "rate": 10000, "per": "campaign"},
            {"name": "Sponsored Stand-up", "rate": 8000, "per": "performance"}
        ],
        "pricing_tier": "$5-25K",
        "audience": "18-34, diverse, comedy enthusiasts",
        "contact_email": "kalani@rezagency.com",
        "contact_phone": "+1-555-0101",
        "rate_card_url": "https://rezagency.com/kalani-ratecard.pdf",
        "brand_partnerships": [
            {"brand": "BetterHelp", "status": "Active", "value": 15000},
            {"brand": "Manscaped", "status": "Active", "value": 8000},
            {"brand": "Square Cash", "status": "Completed", "value": 5000}
        ],
        "recent_posts": [
            {"platform": "tiktok", "date": "2026-03-15", "views": 450000, "engagement": 32000},
            {"platform": "instagram", "date": "2026-03-14", "views": 120000, "engagement": 8900},
            {"platform": "youtube", "date": "2026-03-10", "views": 85000, "engagement": 5200}
        ]
    },
    "2": {
        "id": "2",
        "name": "Coach Canela",
        "niche": "Fitness",
        "bio": "Certified fitness coach specializing in HIIT workouts, nutrition coaching, and fitness motivation. Building a community of health-conscious individuals.",
        "platforms": {
            "instagram": {"handle": "@coachcanela", "followers": 580000, "engagement_rate": 9.1},
            "tiktok": {"handle": "@coachcanela", "followers": 720000, "engagement_rate": 10.3},
            "youtube": {"handle": "Coach Canela", "followers": 420000, "engagement_rate": 8.7},
            "pinterest": {"handle": "coachcanela", "followers": 250000, "engagement_rate": 6.2}
        },
        "total_reach": 1970000,
        "avg_engagement_rate": 8.6,
        "verticals": ["Fitness", "Wellness", "Health", "Apparel"],
        "services": [
            {"name": "Instagram Sponsorship", "rate": 3000, "per": "post"},
            {"name": "TikTok Package (5 videos)", "rate": 8000, "per": "package"},
            {"name": "YouTube Integration", "rate": 6000, "per": "video"},
            {"name": "Fitness Program Collaboration", "rate": 20000, "per": "campaign"}
        ],
        "pricing_tier": "$25-100K",
        "audience": "20-40, fitness-focused, women 65%, men 35%",
        "contact_email": "canela@rezagency.com",
        "contact_phone": "+1-555-0102",
        "rate_card_url": "https://rezagency.com/canela-ratecard.pdf",
        "brand_partnerships": [
            {"brand": "Gymshark", "status": "Active", "value": 45000},
            {"brand": "Alo Yoga", "status": "Active", "value": 35000},
            {"brand": "Celsius Energy", "status": "Completed", "value": 12000},
            {"brand": "Lululemon", "status": "Negotiating", "value": 50000}
        ],
        "recent_posts": [
            {"platform": "tiktok", "date": "2026-03-16", "views": 680000, "engagement": 68000},
            {"platform": "instagram", "date": "2026-03-15", "views": 240000, "engagement": 22000},
            {"platform": "youtube", "date": "2026-03-12", "views": 125000, "engagement": 10800}
        ]
    },
    "3": {
        "id": "3",
        "name": "Kayla Rae Ortiz",
        "niche": "Wellness",
        "bio": "Holistic wellness advocate, meditation instructor, and mental health champion. Focused on sustainable wellness practices and self-care education.",
        "platforms": {
            "instagram": {"handle": "@kaylarae.wellness", "followers": 420000, "engagement_rate": 9.8},
            "tiktok": {"handle": "@kaylarae.wellness", "followers": 580000, "engagement_rate": 11.2},
            "youtube": {"handle": "Kayla Rae Wellness", "followers": 310000, "engagement_rate": 9.5},
            "podcast": {"name": "Wellness Chronicles", "listeners": 85000, "per_episode": 6500}
        },
        "total_reach": 1395000,
        "avg_engagement_rate": 10.1,
        "verticals": ["Wellness", "Health", "Beauty", "Lifestyle"],
        "services": [
            {"name": "Instagram Post", "rate": 2500, "per": "post"},
            {"name": "TikTok Series (3 videos)", "rate": 6000, "per": "series"},
            {"name": "YouTube Collaboration", "rate": 5500, "per": "video"},
            {"name": "Podcast Integration", "rate": 8000, "per": "episode"},
            {"name": "Wellness Workshop", "rate": 15000, "per": "session"}
        ],
        "pricing_tier": "$25-100K",
        "audience": "20-45, wellness-focused, primarily female (72%), eco-conscious",
        "contact_email": "kayla@rezagency.com",
        "contact_phone": "+1-555-0103",
        "rate_card_url": "https://rezagency.com/kayla-ratecard.pdf",
        "brand_partnerships": [
            {"brand": "Calm App", "status": "Active", "value": 25000},
            {"brand": "BetterHelp", "status": "Active", "value": 22000},
            {"brand": "Athletic Greens", "status": "Completed", "value": 18000},
            {"brand": "Oura Ring", "status": "Active", "value": 20000}
        ],
        "recent_posts": [
            {"platform": "tiktok", "date": "2026-03-17", "views": 520000, "engagement": 58000},
            {"platform": "instagram", "date": "2026-03-16", "views": 185000, "engagement": 18150},
            {"platform": "podcast", "date": "2026-03-14", "downloads": 12300, "engagement": 2100}
        ]
    }
}

# Outreach Templates
TEMPLATES = {
    "email": [
        {
            "id": "email_cold_intro",
            "channel": "email",
            "name": "Cold Introduction",
            "subject": "Partnership Opportunity — {{creator_name}} x {{brand_name}}",
            "body": """Hi {{contact_name}},

I hope this email finds you well! I'm reaching out on behalf of {{creator_name}}, an exceptional {{niche}} creator with a highly engaged audience across social media.

**Quick Stats:**
• {{total_reach}}+ followers across platforms
• {{avg_engagement_rate}}% average engagement rate
• Audience: {{audience}}

{{creator_name}} has successfully partnered with brands in your space and would be a great fit for {{brand_name}}. I'd love to explore a potential collaboration that drives real results for your brand.

Would you be open to a brief conversation about partnership opportunities?

Best regards,
Rez | Talent Manager
hello@rezthegiant.com
www.rezagency.com"""
        },
        {
            "id": "email_follow_up",
            "channel": "email",
            "name": "Follow-Up Email",
            "subject": "Re: Partnership Opportunity — {{creator_name}} x {{brand_name}}",
            "body": """Hi {{contact_name}},

I wanted to follow up on my previous email about {{creator_name}} and {{brand_name}}.

Given {{creator_name}}'s strong alignment with {{brand_name}}'s target audience and proven track record with similar brands, I think this could be a really valuable partnership.

Would this week be a good time for a quick 15-minute call to discuss potential collaboration options?

Looking forward to hearing from you!

Best regards,
Rez | Talent Manager
hello@rezthegiant.com"""
        },
        {
            "id": "email_rate_card",
            "channel": "email",
            "name": "Rate Card & Pricing",
            "subject": "{{creator_name}} — Rate Card & Collaboration Options",
            "body": """Hi {{contact_name}},

Thank you for your interest in working with {{creator_name}}!

I'm attaching a detailed rate card that outlines collaboration options, pricing, and deliverables across platforms.

**Quick Overview:**
• Single platform posts: $2,500 - $5,000
• Multi-platform campaigns: $10,000 - $50,000+
• Custom collaborations: Let's discuss your goals

{{creator_name}}'s audience is highly engaged and perfect for {{vertical}} brands. I'm confident we can create a campaign that delivers real ROI.

Would you like to schedule a call to discuss custom package options?

Best regards,
Rez | Talent Manager
hello@rezthegiant.com"""
        },
        {
            "id": "email_multi_talent",
            "channel": "email",
            "name": "Multi-Talent Package",
            "subject": "Multi-Creator Campaign Opportunity — {{brand_name}}",
            "body": """Hi {{contact_name}},

I represent three exceptional creators with diverse audiences and strong engagement metrics:

**Available Talent:**
1. {{creator_name}} — {{niche}} ({{total_reach}}+ followers)
2. [Additional creator details available upon request]

We can structure a comprehensive campaign that leverages multiple platforms and audience segments to maximize your brand's reach and impact.

**Package Benefits:**
• Cross-platform amplification
• Diverse audience reach
• Authentic brand advocacy
• Flexible pricing and terms

Would you be interested in exploring a multi-creator campaign for {{brand_name}}?

Best regards,
Rez | Talent Manager
hello@rezthegiant.com"""
        },
        {
            "id": "email_seasonal",
            "channel": "email",
            "name": "Seasonal Campaign Pitch",
            "subject": "Spring Campaign Opportunity — {{creator_name}} x {{brand_name}}",
            "body": """Hi {{contact_name}},

With the season changing, it's the perfect time to refresh {{brand_name}}'s social strategy. {{creator_name}} is launching a seasonal content series and I think {{brand_name}} could be a perfect fit.

**Campaign Details:**
• 5-8 content pieces across platforms
• Peak posting times for maximum reach
• {{total_reach}}+ potential impressions
• {{avg_engagement_rate}}% engagement baseline

We can customize the approach to align with {{brand_name}}'s goals and timeline. Typically, these campaigns drive strong awareness and conversion results.

Are you interested in a quick conversation to explore this opportunity?

Best regards,
Rez | Talent Manager
hello@rezthegiant.com"""
        },
        {
            "id": "email_post_meeting",
            "channel": "email",
            "name": "Post-Meeting Proposal",
            "subject": "Following Up on Our Conversation — {{brand_name}} Partnership",
            "body": """Hi {{contact_name}},

Thank you for taking the time to discuss partnership opportunities with {{creator_name}} and {{brand_name}}. It was great to learn more about your goals and vision.

Based on our conversation, I've outlined a custom proposal that I believe hits the mark:

**Proposed Deliverables:**
• {{total_reach}}+ audience reach
• {{avg_engagement_rate}}% engagement rate
• Multi-platform content series
• Performance tracking and reporting

I'm excited about the potential for this partnership. When you have a chance, please review the attached proposal and let me know your thoughts.

Looking forward to moving forward together!

Best regards,
Rez | Talent Manager
hello@rezthegiant.com"""
        },
        {
            "id": "email_deal_proposal",
            "channel": "email",
            "name": "Deal Proposal & Terms",
            "subject": "Partnership Proposal — {{creator_name}} & {{brand_name}} Deal Terms",
            "body": """Hi {{contact_name}},

I'm excited to send over the formal proposal for the {{creator_name}} and {{brand_name}} partnership.

**Deal Summary:**
• Campaign Duration: [TBD based on deliverables]
• Content Deliverables: [Custom to your needs]
• Budget: [Per attached proposal]
• Timeline: [TBD]

This package is designed to maximize {{brand_name}}'s visibility and drive measurable results with {{creator_name}}'s engaged audience.

Please review and let me know if you'd like to discuss any adjustments to the terms or deliverables.

I'm available for a call at your earliest convenience.

Best regards,
Rez | Talent Manager
hello@rezthegiant.com"""
        }
    ],
    "instagram_dm": [
        {
            "id": "ig_cold_intro",
            "channel": "instagram_dm",
            "name": "Cold Introduction",
            "body": "Hey {{contact_name}}! 👋 I represent {{creator_name}}, an amazing {{niche}} creator with {{total_reach}}+ followers. Their audience is super engaged ({{avg_engagement_rate}}% engagement) and would be a great fit for {{brand_name}}. Would love to explore a potential collab! DM back if interested 🙌"
        },
        {
            "id": "ig_follow_up",
            "channel": "instagram_dm",
            "name": "Follow-Up Message",
            "body": "Hey {{contact_name}}, just following up on my previous message about {{creator_name}}. Think they'd be a great partner for {{brand_name}}'s upcoming campaigns. Let me know if you'd like to chat more! 💬"
        },
        {
            "id": "ig_collab_pitch",
            "channel": "instagram_dm",
            "name": "Collaboration Pitch",
            "body": "Hi {{contact_name}}! {{creator_name}} is interested in collaborating with {{brand_name}}. They have a highly engaged {{audience}} audience and create awesome {{niche}} content. Would love to set up a quick call to discuss! Are you open to it?"
        },
        {
            "id": "ig_story_offer",
            "channel": "instagram_dm",
            "name": "Story Mention Offer",
            "body": "Hey {{contact_name}}! {{creator_name}} would love to feature {{brand_name}} in their stories to their {{total_reach}}+ followers. Perfect for increasing brand awareness! Would you be interested? Let's chat! 🎉"
        }
    ],
    "linkedin": [
        {
            "id": "linkedin_connection",
            "channel": "linkedin",
            "name": "Connection Request Note",
            "body": "Hi {{contact_name}}, I'm Rez, a talent manager representing {{creator_name}}, an influential {{niche}} creator with {{total_reach}}+ followers. I see {{brand_name}} is expanding in the {{vertical}} space and think {{creator_name}} would be a great partnership fit. Would love to connect and explore collaboration opportunities!"
        },
        {
            "id": "linkedin_inmail",
            "channel": "linkedin",
            "name": "InMail Introduction",
            "subject": "Talent Partnership Opportunity — {{creator_name}} x {{brand_name}}",
            "body": "Hi {{contact_name}},\n\nI'm reaching out because I believe {{creator_name}} would be an excellent partner for {{brand_name}}'s marketing initiatives.\n\n**Why {{creator_name}}?**\n• {{total_reach}}+ engaged followers\n• {{avg_engagement_rate}}% engagement rate\n• Proven results with {{vertical}} brands\n• Authentic audience alignment\n\nWould you be open to a brief conversation about how we can drive results together?\n\nBest regards,\nRez"
        },
        {
            "id": "linkedin_followup",
            "channel": "linkedin",
            "name": "Connection Follow-Up",
            "body": "Hi {{contact_name}}, following up on my previous message about {{creator_name}}. I think this partnership could deliver real value for {{brand_name}}. Would appreciate 15 minutes of your time to discuss. Thanks!"
        }
    ],
    "tiktok": [
        {
            "id": "tiktok_cold_dm",
            "channel": "tiktok",
            "name": "Cold DM",
            "body": "Hey {{contact_name}}! 👋 {{creator_name}} here representing talent partnerships for {{brand_name}}. {{creator_name}} is an awesome {{niche}} creator with {{total_reach}}+ followers and would be perfect for a collab. Interested? Let's chat! 🙌"
        },
        {
            "id": "tiktok_collab",
            "channel": "tiktok",
            "name": "Collaboration Pitch",
            "body": "Hey {{contact_name}}! {{creator_name}} wants to create some fire content with {{brand_name}}. {{total_reach}}+ followers, {{avg_engagement_rate}}% engagement. Perfect audience match for your brand. You down? 🔥"
        },
        {
            "id": "tiktok_trending",
            "channel": "tiktok",
            "name": "Trending Content Hook",
            "body": "{{contact_name}}, {{creator_name}} is hopping on the trending {{niche}} wave and thinks {{brand_name}} should be part of it! {{avg_engagement_rate}}% engagement rate = tons of visibility. Let's make viral content together! 🚀"
        }
    ]
}

# Seed Brands Data
SEED_BRANDS = [
    {"name": "Glossier", "vertical": "Beauty", "budget_tier": "$25-100K", "contact_name": "Sarah Chen", "contact_email": "partnerships@glossier.com", "contact_title": "Head of Influencer Partnerships", "website": "glossier.com", "instagram": "@glossier", "linkedin": "glossier", "tiktok": "@glossier"},
    {"name": "Gymshark", "vertical": "Fitness/Apparel", "budget_tier": "$100K+", "contact_name": "Marcus Williams", "contact_email": "influencer@gymshark.com", "contact_title": "Director of Creator Relations", "website": "gymshark.com", "instagram": "@gymshark", "linkedin": "gymshark", "tiktok": "@gymshark"},
    {"name": "Alo Yoga", "vertical": "Fitness/Wellness", "budget_tier": "$100K+", "contact_name": "Jessica Rodriguez", "contact_email": "creators@aloyoga.com", "contact_title": "Influencer Manager", "website": "aloyoga.com", "instagram": "@aloyoga", "linkedin": "alo-yoga", "tiktok": "@aloyoga"},
    {"name": "Fashion Nova", "vertical": "Fashion", "budget_tier": "$100K+", "contact_name": "Brandon Lee", "contact_email": "partnerships@fashionnova.com", "contact_title": "Talent Manager", "website": "fashionnova.com", "instagram": "@fashionnova", "linkedin": "fashion-nova", "tiktok": "@fashionnova"},
    {"name": "Fabletics", "vertical": "Fitness/Apparel", "budget_tier": "$25-100K", "contact_name": "Nicole Adams", "contact_email": "creators@fabletics.com", "contact_title": "Partnership Coordinator", "website": "fabletics.com", "instagram": "@fabletics", "linkedin": "fabletics", "tiktok": "@fabletics"},
    {"name": "Lululemon", "vertical": "Fitness/Apparel", "budget_tier": "$100K+", "contact_name": "David Park", "contact_email": "partnerships@lululemon.com", "contact_title": "Creator Marketing Manager", "website": "lululemon.com", "instagram": "@lululemon", "linkedin": "lululemon", "tiktok": "@lululemon"},
    {"name": "Nike", "vertical": "Sports/Fitness", "budget_tier": "$100K+", "contact_name": "Ashley Thompson", "contact_email": "athlete-partnerships@nike.com", "contact_title": "Influencer Relations Lead", "website": "nike.com", "instagram": "@nike", "linkedin": "nike", "tiktok": "@nike"},
    {"name": "Sephora", "vertical": "Beauty", "budget_tier": "$100K+", "contact_name": "Michelle Zhang", "contact_email": "influencers@sephora.com", "contact_title": "Head of Social Marketing", "website": "sephora.com", "instagram": "@sephora", "linkedin": "sephora", "tiktok": "@sephora"},
    {"name": "Savage X Fenty", "vertical": "Fashion/Beauty", "budget_tier": "$100K+", "contact_name": "Keisha Brown", "contact_email": "partnerships@savagex.com", "contact_title": "Brand Partnerships Manager", "website": "savagex.fenty.com", "instagram": "@savagex", "linkedin": "rihanna-fenty", "tiktok": "@savagex"},
    {"name": "PrettyLittleThing", "vertical": "Fashion", "budget_tier": "$25-100K", "contact_name": "Amara Johnson", "contact_email": "partnerships@prettylittlething.com", "contact_title": "Influencer Coordinator", "website": "prettylittlething.com", "instagram": "@prettylittlething", "linkedin": "prettylittlething", "tiktok": "@prettylittlething"},
    {"name": "Celsius Energy", "vertical": "Food & Beverage", "budget_tier": "$25-100K", "contact_name": "Tyler Martinez", "contact_email": "partnerships@celsius.com", "contact_title": "Creator Relations Manager", "website": "celsius.com", "instagram": "@celsius", "linkedin": "celsius-energy-drink", "tiktok": "@celsius"},
    {"name": "Bloom Nutrition", "vertical": "Supplements", "budget_tier": "$5-25K", "contact_name": "Lauren Foster", "contact_email": "collaborations@bloom.co", "contact_title": "Brand Partnerships", "website": "bloomnutrition.com", "instagram": "@bloomnutrition", "linkedin": "bloom-nutrition", "tiktok": "@bloomnutrition"},
    {"name": "Liquid IV", "vertical": "Health/Beverage", "budget_tier": "$25-100K", "contact_name": "Chris Anderson", "contact_email": "influencers@liquidiv.com", "contact_title": "Partnerships Manager", "website": "liquid-iv.com", "instagram": "@liquidiv", "linkedin": "liquid-iv", "tiktok": "@liquidiv"},
    {"name": "HelloFresh", "vertical": "Food", "budget_tier": "$100K+", "contact_name": "Rebecca Cohen", "contact_email": "influencer-team@hellofresh.com", "contact_title": "Influencer Marketing Manager", "website": "hellofresh.com", "instagram": "@hellofresh", "linkedin": "hellofresh", "tiktok": "@hellofresh"},
    {"name": "BetterHelp", "vertical": "Wellness/Health", "budget_tier": "$100K+", "contact_name": "Dr. James Wilson", "contact_email": "partnerships@betterhelp.com", "contact_title": "Director of Brand Partnerships", "website": "betterhelp.com", "instagram": "@betterhelp", "linkedin": "betterhelp", "tiktok": "@betterhelp"},
    {"name": "Calm App", "vertical": "Wellness/Tech", "budget_tier": "$25-100K", "contact_name": "Emma Davis", "contact_email": "partnerships@calm.com", "contact_title": "Brand Partnerships Lead", "website": "calm.com", "instagram": "@calm", "linkedin": "calm", "tiktok": "@calm"},
    {"name": "Oura Ring", "vertical": "Tech/Wellness", "budget_tier": "$25-100K", "contact_name": "Sophie Turner", "contact_email": "partnerships@ouraring.com", "contact_title": "Creator Partnerships Manager", "website": "ouraring.com", "instagram": "@ouraring", "linkedin": "oura", "tiktok": "@ouraring"},
    {"name": "Athletic Greens", "vertical": "Supplements", "budget_tier": "$25-100K", "contact_name": "Michael Khan", "contact_email": "partnerships@athleticgreens.com", "contact_title": "Influencer Relations", "website": "athleticgreens.com", "instagram": "@athleticgreens", "linkedin": "athletic-greens", "tiktok": "@athleticgreens"},
    {"name": "Skims", "vertical": "Fashion", "budget_tier": "$100K+", "contact_name": "Priya Patel", "contact_email": "collaborations@skims.com", "contact_title": "Creator Relations Manager", "website": "skims.com", "instagram": "@skims", "linkedin": "skims", "tiktok": "@skims"},
    {"name": "Rare Beauty", "vertical": "Beauty", "budget_tier": "$100K+", "contact_name": "Olivia Grant", "contact_email": "partnerships@rarebeauty.com", "contact_title": "Brand Partnerships Manager", "website": "rarebeauty.com", "instagram": "@rarebeauty", "linkedin": "rare-beauty", "tiktok": "@rarebeauty"},
    {"name": "Drunk Elephant", "vertical": "Beauty/Skincare", "budget_tier": "$25-100K", "contact_name": "Ryan Kelly", "contact_email": "influencer@drunkelephant.com", "contact_title": "Partnerships Coordinator", "website": "drunkelephant.com", "instagram": "@drunkelephant", "linkedin": "drunk-elephant", "tiktok": "@drunkelephant"},
    {"name": "Manduka", "vertical": "Yoga/Fitness", "budget_tier": "$5-25K", "contact_name": "Sophia Martinez", "contact_email": "partnerships@manduka.com", "contact_title": "Creator Relations", "website": "manduka.com", "instagram": "@manduka", "linkedin": "manduka", "tiktok": "@manduka"},
    {"name": "Vuori", "vertical": "Apparel/Fitness", "budget_tier": "$25-100K", "contact_name": "Jordan Hayes", "contact_email": "partnerships@vuori.com", "contact_title": "Influencer Manager", "website": "vuori.com", "instagram": "@vuoriclothing", "linkedin": "vuori", "tiktok": "@vuoriclothing"},
    {"name": "Poppi", "vertical": "Food & Beverage", "budget_tier": "$5-25K", "contact_name": "Alexandra Blue", "contact_email": "partnerships@poppi.com", "contact_title": "Brand Partnerships", "website": "poppi.com", "instagram": "@poppibeverage", "linkedin": "poppi", "tiktok": "@poppibeverage"},
    {"name": "Olipop", "vertical": "Food & Beverage", "budget_tier": "$5-25K", "contact_name": "Marcus Green", "contact_email": "partnerships@olipop.com", "contact_title": "Creator Relations Manager", "website": "olipop.com", "instagram": "@olipop", "linkedin": "olipop", "tiktok": "@olipop"}
]

# Creator platform handles for API lookups
CREATOR_HANDLES = {
    "1": {  # Kalani Rodgers - Comedy
        "youtube": "KalaniRodgers",
        "twitch": "kalanirodgers",
        "kick": "kalanirodgers",
        "instagram": "kalanirodgers",
        "tiktok": "kalanirodgers",
        "x": "kalanirodgers",
    },
    "2": {  # Coach Canela - Fitness
        "youtube": "CoachCanela",
        "twitch": "coachcanela",
        "kick": "coachcanela",
        "instagram": "coachcanela",
        "tiktok": "coachcanela",
        "x": "coachcanela",
    },
    "3": {  # Kayla Rae Ortiz - Wellness
        "youtube": "KaylaRaeWellness",
        "twitch": "kaylaraewellness",
        "kick": "kaylaraewellness",
        "instagram": "kaylarae.wellness",
        "tiktok": "kaylarae.wellness",
        "x": "kaylaraewellness",
    },
}


class PlatformAPI:
    """Unified social media platform data fetcher with SQLite caching.

    Supports: YouTube (Data API v3), Twitch (Helix API), Kick (public API),
    Instagram (Graph API), X/Twitter (API v2), TikTok (Research API).
    Falls back to embedded mock data when API keys are not configured.
    """

    CACHE_TTL_SECONDS = 3600  # Cache results for 1 hour

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._twitch_token: Optional[str] = None
        self._twitch_token_expires: float = 0.0

    def _get_db(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _get_cached(self, creator_id: str, platform: str) -> Optional[Dict]:
        """Return cached data if still within TTL, else None."""
        try:
            conn = self._get_db()
            c = conn.cursor()
            c.execute(
                'SELECT data, fetched_at FROM platform_api_cache '
                'WHERE creator_id = ? AND platform = ? ORDER BY fetched_at DESC LIMIT 1',
                (creator_id, platform)
            )
            row = c.fetchone()
            conn.close()
            if row:
                data_str, fetched_at_str = row
                fetched_at = datetime.fromisoformat(fetched_at_str)
                age = (datetime.utcnow() - fetched_at).total_seconds()
                if age < self.CACHE_TTL_SECONDS:
                    return json.loads(data_str)
        except Exception:
            pass
        return None

    def _set_cached(self, creator_id: str, platform: str, data: Dict, source: str = 'api') -> None:
        """Upsert data into the platform_api_cache table."""
        try:
            conn = self._get_db()
            c = conn.cursor()
            c.execute('DELETE FROM platform_api_cache WHERE creator_id = ? AND platform = ?',
                      (creator_id, platform))
            c.execute(
                'INSERT INTO platform_api_cache (id, creator_id, platform, data, fetched_at, source) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (str(uuid.uuid4()), creator_id, platform, json.dumps(data),
                 datetime.utcnow().isoformat(), source)
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    def _http_get(self, url: str, headers: Optional[Dict] = None, timeout: int = 10) -> Optional[Dict]:
        """HTTP GET with urllib.request; returns parsed JSON or None on error."""
        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'AgencyOutreachBot/1.0')
            if headers:
                for k, v in headers.items():
                    req.add_header(k, v)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception:
            return None

    def _http_post_form(self, url: str, params: Dict, timeout: int = 10) -> Optional[Dict]:
        """HTTP POST with form-encoded body; returns parsed JSON or None."""
        try:
            from urllib.parse import urlencode
            data = urlencode(params).encode('utf-8')
            req = urllib.request.Request(url, data=data, method='POST')
            req.add_header('Content-Type', 'application/x-www-form-urlencoded')
            req.add_header('User-Agent', 'AgencyOutreachBot/1.0')
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception:
            return None

    # ──────────────────────────────── YouTube ────────────────────────────────

    def fetch_youtube(self, creator_id: str, handle: str) -> Dict:
        """Fetch YouTube channel stats using Data API v3 (free, 10K units/day)."""
        cached = self._get_cached(creator_id, 'youtube')
        if cached:
            return cached

        if not YOUTUBE_API_KEY:
            return self._fallback(creator_id, 'youtube')

        clean = handle.lstrip('@')
        # Try by @handle first (modern channels)
        data = self._http_get(
            f'https://www.googleapis.com/youtube/v3/channels'
            f'?part=statistics,snippet&forHandle={clean}&key={YOUTUBE_API_KEY}'
        )
        if not data or not data.get('items'):
            # Fallback: try legacy username
            data = self._http_get(
                f'https://www.googleapis.com/youtube/v3/channels'
                f'?part=statistics,snippet&forUsername={clean}&key={YOUTUBE_API_KEY}'
            )

        if data and data.get('items'):
            ch = data['items'][0]
            stats = ch.get('statistics', {})
            snippet = ch.get('snippet', {})
            channel_id = ch.get('id', '')
            avg_views = self._youtube_avg_views(channel_id) if channel_id else 0
            result = {
                'platform': 'youtube',
                'followers': int(stats.get('subscriberCount', 0)),
                'total_views': int(stats.get('viewCount', 0)),
                'video_count': int(stats.get('videoCount', 0)),
                'avg_views_30d': avg_views,
                'channel_name': snippet.get('title', clean),
                'source': 'api',
                'fetched_at': datetime.utcnow().isoformat(),
            }
            self._set_cached(creator_id, 'youtube', result)
            return result

        return self._fallback(creator_id, 'youtube')

    def _youtube_avg_views(self, channel_id: str) -> int:
        """Average views of videos published in the last 30 days (uses ~101 quota units)."""
        if not YOUTUBE_API_KEY or not channel_id:
            return 0
        after = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
        search = self._http_get(
            f'https://www.googleapis.com/youtube/v3/search'
            f'?part=id&channelId={channel_id}&type=video&order=date'
            f'&maxResults=10&publishedAfter={after}&key={YOUTUBE_API_KEY}'
        )
        if not search or not search.get('items'):
            return 0
        ids = [i['id']['videoId'] for i in search['items'] if i.get('id', {}).get('videoId')]
        if not ids:
            return 0
        videos = self._http_get(
            f'https://www.googleapis.com/youtube/v3/videos'
            f'?part=statistics&id={",".join(ids)}&key={YOUTUBE_API_KEY}'
        )
        if not videos or not videos.get('items'):
            return 0
        total = sum(int(v.get('statistics', {}).get('viewCount', 0)) for v in videos['items'])
        return total // len(videos['items'])

    # ──────────────────────────────── Twitch ─────────────────────────────────

    def _twitch_app_token(self) -> Optional[str]:
        """Get or refresh Twitch app access token via client credentials flow."""
        if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
            return None
        if self._twitch_token and time.time() < self._twitch_token_expires:
            return self._twitch_token
        resp = self._http_post_form(
            'https://id.twitch.tv/oauth2/token',
            {'client_id': TWITCH_CLIENT_ID,
             'client_secret': TWITCH_CLIENT_SECRET,
             'grant_type': 'client_credentials'}
        )
        if resp and resp.get('access_token'):
            self._twitch_token = resp['access_token']
            self._twitch_token_expires = time.time() + resp.get('expires_in', 3600) - 60
            return self._twitch_token
        return None

    def fetch_twitch(self, creator_id: str, username: str) -> Dict:
        """Fetch Twitch follower count + total views via Helix API (free)."""
        cached = self._get_cached(creator_id, 'twitch')
        if cached:
            return cached

        token = self._twitch_app_token()
        if not token:
            return self._fallback(creator_id, 'twitch')

        headers = {'Authorization': f'Bearer {token}', 'Client-Id': TWITCH_CLIENT_ID}

        # Step 1: Get broadcaster_id
        user_data = self._http_get(
            f'https://api.twitch.tv/helix/users?login={username}', headers=headers
        )
        if not user_data or not user_data.get('data'):
            return self._fallback(creator_id, 'twitch')

        user = user_data['data'][0]
        broadcaster_id = user['id']

        # Step 2: Get follower total (app token returns only the count, not list)
        followers_data = self._http_get(
            f'https://api.twitch.tv/helix/channels/followers?broadcaster_id={broadcaster_id}',
            headers=headers
        )
        follower_count = followers_data.get('total', 0) if followers_data else 0

        result = {
            'platform': 'twitch',
            'followers': follower_count,
            'total_views': user.get('view_count', 0),
            'display_name': user.get('display_name', username),
            'broadcaster_type': user.get('broadcaster_type', ''),
            'source': 'api',
            'fetched_at': datetime.utcnow().isoformat(),
        }
        self._set_cached(creator_id, 'twitch', result)
        return result

    # ──────────────────────────────── Kick ───────────────────────────────────

    def fetch_kick(self, creator_id: str, username: str) -> Dict:
        """Fetch Kick channel stats via public API endpoint."""
        cached = self._get_cached(creator_id, 'kick')
        if cached:
            return cached

        data = self._http_get(
            f'https://kick.com/api/v2/channels/{username}',
            headers={'Accept': 'application/json'}
        )
        if data and not data.get('error'):
            result = {
                'platform': 'kick',
                'followers': data.get('followersCount', data.get('followers_count', 0)),
                'total_views': data.get('viewersCount', data.get('viewers_count', 0)),
                'is_live': data.get('livestream') is not None,
                'username': data.get('slug', username),
                'source': 'api',
                'fetched_at': datetime.utcnow().isoformat(),
            }
            self._set_cached(creator_id, 'kick', result)
            return result

        return self._fallback(creator_id, 'kick')

    # ──────────────────────────────── Instagram ───────────────────────────────

    def fetch_instagram(self, creator_id: str, username: str) -> Dict:
        """Fetch Instagram stats via Graph API (requires INSTAGRAM_ACCESS_TOKEN)."""
        cached = self._get_cached(creator_id, 'instagram')
        if cached:
            return cached

        if not INSTAGRAM_ACCESS_TOKEN:
            return self._fallback(creator_id, 'instagram')

        data = self._http_get(
            f'https://graph.instagram.com/me'
            f'?fields=followers_count,media_count,username'
            f'&access_token={INSTAGRAM_ACCESS_TOKEN}'
        )
        if data and not data.get('error'):
            result = {
                'platform': 'instagram',
                'followers': data.get('followers_count', 0),
                'post_count': data.get('media_count', 0),
                'username': data.get('username', username),
                'source': 'api',
                'fetched_at': datetime.utcnow().isoformat(),
            }
            self._set_cached(creator_id, 'instagram', result)
            return result

        return self._fallback(creator_id, 'instagram')

    # ──────────────────────────────── X (Twitter) ────────────────────────────

    def fetch_x(self, creator_id: str, username: str) -> Dict:
        """Fetch X public metrics via API v2 (requires X_BEARER_TOKEN, Basic tier)."""
        cached = self._get_cached(creator_id, 'x')
        if cached:
            return cached

        if not X_BEARER_TOKEN:
            return self._fallback(creator_id, 'x')

        clean = username.lstrip('@')
        data = self._http_get(
            f'https://api.twitter.com/2/users/by/username/{clean}?user.fields=public_metrics',
            headers={'Authorization': f'Bearer {X_BEARER_TOKEN}'}
        )
        if data and data.get('data'):
            metrics = data['data'].get('public_metrics', {})
            result = {
                'platform': 'x',
                'followers': metrics.get('followers_count', 0),
                'following': metrics.get('following_count', 0),
                'tweet_count': metrics.get('tweet_count', 0),
                'username': clean,
                'source': 'api',
                'fetched_at': datetime.utcnow().isoformat(),
            }
            self._set_cached(creator_id, 'x', result)
            return result

        return self._fallback(creator_id, 'x')

    # ──────────────────────────────── TikTok ─────────────────────────────────

    def fetch_tiktok(self, creator_id: str, username: str) -> Dict:
        """Fetch TikTok stats (requires TIKTOK_ACCESS_TOKEN from Research API approval)."""
        cached = self._get_cached(creator_id, 'tiktok')
        if cached:
            return cached
        # TikTok Research API requires application approval at developers.tiktok.com
        # Falls back to mock data until TIKTOK_ACCESS_TOKEN is configured
        return self._fallback(creator_id, 'tiktok')

    # ──────────────────────────────── Fallback ───────────────────────────────

    def _fallback(self, creator_id: str, platform: str) -> Dict:
        """Return embedded mock stats from the CREATORS dict."""
        creator = CREATORS.get(creator_id, {})
        pdata = creator.get('platforms', {}).get(platform, {})
        result = {
            'platform': platform,
            'followers': pdata.get('followers', 0),
            'engagement_rate': pdata.get('engagement_rate', 0),
            'source': 'mock',
            'fetched_at': datetime.utcnow().isoformat(),
        }
        self._set_cached(creator_id, platform, result, source='mock')
        return result

    # ──────────────────────────────── Aggregate ──────────────────────────────

    def fetch_all(self, creator_id: str, force: bool = False) -> Dict:
        """Fetch stats for every platform for a creator.

        Args:
            creator_id: Creator ID string ("1", "2", or "3")
            force: If True, clear cached data and fetch fresh
        Returns:
            Dict mapping platform name -> stats dict
        """
        creator = CREATORS.get(creator_id, {})
        creator_platforms = set(creator.get('platforms', {}).keys())
        handles = CREATOR_HANDLES.get(creator_id, {})

        fetch_map = {
            'youtube': self.fetch_youtube,
            'twitch': self.fetch_twitch,
            'kick': self.fetch_kick,
            'tiktok': self.fetch_tiktok,
            'instagram': self.fetch_instagram,
            'x': self.fetch_x,
        }

        results = {}
        for platform, fetch_fn in fetch_map.items():
            handle = handles.get(platform, '')
            # Only fetch platforms the creator is on (plus Twitch/Kick as bonus)
            if platform not in creator_platforms and platform not in ('twitch', 'kick'):
                continue
            if force:
                try:
                    conn = self._get_db()
                    c = conn.cursor()
                    c.execute(
                        'DELETE FROM platform_api_cache WHERE creator_id = ? AND platform = ?',
                        (creator_id, platform)
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
            try:
                results[platform] = fetch_fn(creator_id, handle)
            except Exception:
                results[platform] = self._fallback(creator_id, platform)

        return results

    def generate_rate_card(self, creator_id: str) -> Dict:
        """Generate a rate card with pricing tiers derived from real platform metrics."""
        creator = CREATORS.get(creator_id)
        if not creator:
            return {'error': 'Creator not found'}

        all_stats = self.fetch_all(creator_id)

        # Aggregate follower count across all platforms
        total_followers = sum(s.get('followers', 0) for s in all_stats.values())

        # Weighted average engagement rate
        eng_rates = []
        for platform, s in all_stats.items():
            followers = s.get('followers', 0)
            rate = s.get('engagement_rate') or creator['platforms'].get(platform, {}).get('engagement_rate', 0)
            if followers > 0 and rate > 0:
                eng_rates.append(rate)
        avg_engagement = round(sum(eng_rates) / len(eng_rates), 2) if eng_rates else creator.get('avg_engagement_rate', 5.0)

        def calc_price(followers: int, eng_rate: float, content_type: str) -> int:
            """CPM-based pricing with engagement multiplier."""
            base_cpm = 15.0  # $15 per 1,000 followers baseline
            eng_mult = 1.0 + (eng_rate / 10.0)
            base = (followers / 1000.0) * base_cpm * eng_mult
            mults = {
                'story': 0.25, 'post': 1.0, 'reel': 1.3,
                'video': 2.0, 'series': 4.5, 'campaign': 9.0,
            }
            return max(500, int(base * mults.get(content_type, 1.0)))

        platform_rates = {}
        for platform, stats in all_stats.items():
            followers = stats.get('followers', 0)
            if followers <= 0:
                continue
            eng = stats.get('engagement_rate') or creator['platforms'].get(platform, {}).get('engagement_rate', 5.0)
            platform_rates[platform] = {
                'followers': followers,
                'engagement_rate': eng,
                'source': stats.get('source', 'mock'),
                'rates': {
                    'story': f'${calc_price(followers, eng, "story"):,}',
                    'post': f'${calc_price(followers, eng, "post"):,}',
                    'reel_or_video': f'${calc_price(followers, eng, "reel"):,}',
                    'series_3_videos': f'${calc_price(followers, eng, "series"):,}',
                    'full_campaign': f'${calc_price(followers, eng, "campaign"):,}',
                },
            }

        # Determine overall pricing tier
        if total_followers >= 5_000_000:
            tier = '$100K+'
        elif total_followers >= 1_000_000:
            tier = '$25-100K'
        elif total_followers >= 500_000:
            tier = '$10-25K'
        elif total_followers >= 100_000:
            tier = '$5-10K'
        else:
            tier = '$1-5K'

        return {
            'creator_id': creator_id,
            'creator_name': creator['name'],
            'niche': creator.get('niche', ''),
            'total_reach': total_followers,
            'avg_engagement_rate': avg_engagement,
            'pricing_tier': tier,
            'audience': creator.get('audience', ''),
            'verticals': creator.get('verticals', []),
            'platform_rates': platform_rates,
            'data_sources': {p: s.get('source', 'mock') for p, s in all_stats.items()},
            'generated_at': datetime.utcnow().isoformat(),
        }


# Global PlatformAPI instance (initialized after DB path is known)
_platform_api: Optional['PlatformAPI'] = None


def get_platform_api() -> 'PlatformAPI':
    global _platform_api
    if _platform_api is None:
        _platform_api = PlatformAPI(DB_PATH)
    return _platform_api


# ───────────────────────────────── Twitch API & Ingestion ───────────────────────────────────

class TwitchAPI:
    """Twitch Helix API client for ingesting and searching top streamers.
    Uses only urllib.request (no external dependencies).
    """

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0.0

    def _http_post(self, url: str, params: Dict, headers: Optional[Dict] = None, timeout: int = 10) -> Optional[Dict]:
        """HTTP POST with form-encoded body; returns parsed JSON or None."""
        try:
            from urllib.parse import urlencode
            data = urlencode(params).encode('utf-8')
            req = urllib.request.Request(url, data=data, method='POST')
            req.add_header('Content-Type', 'application/x-www-form-urlencoded')
            req.add_header('User-Agent', 'AgencyOutreachBot/1.0')
            if headers:
                for k, v in headers.items():
                    req.add_header(k, v)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            print(f'Twitch POST error ({url}): {e}', file=__import__('sys').stderr)
            return None

    def _http_get(self, url: str, headers: Optional[Dict] = None, timeout: int = 10) -> Optional[Dict]:
        """HTTP GET with urllib.request; returns parsed JSON or None on error."""
        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'AgencyOutreachBot/1.0')
            if headers:
                for k, v in headers.items():
                    req.add_header(k, v)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            print(f'Twitch GET error ({url}): {e}', file=__import__('sys').stderr)
            return None

    def get_app_token(self) -> Optional[str]:
        """Get Twitch application access token using client credentials flow.
        Token is cached and reused for ~60 days.
        """
        now = time.time()
        if self.access_token and now < self.token_expires_at:
            return self.access_token

        url = 'https://id.twitch.tv/oauth2/token'
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        }
        resp = self._http_post(url, params)
        if not resp or 'access_token' not in resp:
            return None

        self.access_token = resp['access_token']
        expires_in = resp.get('expires_in', 3600)
        self.token_expires_at = now + expires_in - 300  # Refresh 5 min before expiry
        print(f'Twitch token acquired (expires in {expires_in}s)', file=__import__('sys').stderr)
        return self.access_token

    def get_top_streams(self, count: int = 1000) -> List[Dict]:
        """Paginate through top live streams.
        Returns list of stream objects with user_id, user_login, user_name, game_name, viewer_count, etc.
        """
        token = self.get_app_token()
        if not token:
            return []

        headers = {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {token}'
        }

        streams = []
        cursor = None

        while len(streams) < count:
            url = f'https://api.twitch.tv/helix/streams?first=100'
            if cursor:
                url += f'&after={cursor}'

            resp = self._http_get(url, headers)
            if not resp or 'data' not in resp:
                break

            data = resp['data']
            if not data:
                break

            streams.extend(data)

            # Check for pagination
            pagination = resp.get('pagination', {})
            cursor = pagination.get('cursor')
            if not cursor:
                break

            time.sleep(0.1)  # Rate limiting

        return streams[:count]

    def get_users(self, user_ids: List[str]) -> List[Dict]:
        """Batch lookup users by IDs (max 100 per request).
        Returns list of user objects with id, login, display_name, description, profile_image_url, created_at, broadcaster_type.
        """
        token = self.get_app_token()
        if not token or not user_ids:
            return []

        headers = {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {token}'
        }

        users = []
        for i in range(0, len(user_ids), 100):
            batch = user_ids[i:i+100]
            params = '&'.join([f'id={uid}' for uid in batch])
            url = f'https://api.twitch.tv/helix/users?{params}'

            resp = self._http_get(url, headers)
            if resp and 'data' in resp:
                users.extend(resp['data'])

            time.sleep(0.1)

        return users

    def get_follower_count(self, broadcaster_id: str) -> int:
        """Get follower count for a broadcaster.
        Requires app access token (no user scope needed).
        Returns the 'total' field.
        """
        token = self.get_app_token()
        if not token:
            return 0

        headers = {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {token}'
        }

        url = f'https://api.twitch.tv/helix/channels/followers?broadcaster_id={broadcaster_id}&first=1'
        resp = self._http_get(url, headers)

        if resp and 'total' in resp:
            return resp['total']

        return 0

    def search_channels(self, query: str, first: int = 20) -> List[Dict]:
        """Search for channels by query string.
        Returns matching channel objects.
        """
        token = self.get_app_token()
        if not token:
            return []

        headers = {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {token}'
        }

        from urllib.parse import quote
        encoded_query = quote(query)
        url = f'https://api.twitch.tv/helix/search/channels?query={encoded_query}&first={first}'
        resp = self._http_get(url, headers)

        if resp and 'data' in resp:
            return resp['data']

        return []

    def ingest_top_streamers(self, count: int = 2000) -> Dict:
        """Main ingestion method: fetch top streams, get user details, follower counts.
        Store everything in SQLite twitch_creators table.
        Returns dict with ingestion stats.
        """
        print(f'Starting Twitch ingestion (target: {count} creators)', file=__import__('sys').stderr)

        # Get top streams
        streams = self.get_top_streams(count)
        print(f'Fetched {len(streams)} top streams', file=__import__('sys').stderr)

        if not streams:
            return {'total_creators': 0, 'errors': 1, 'message': 'Failed to fetch streams'}

        # Extract user IDs and map stream data
        user_ids = [s['user_id'] for s in streams]
        stream_map = {s['user_id']: s for s in streams}

        # Get user details (batch)
        users = self.get_users(user_ids)
        print(f'Fetched details for {len(users)} users', file=__import__('sys').stderr)

        # Get follower counts (with rate limiting)
        user_map = {u['id']: u for u in users}
        followers_map = {}
        errors = 0

        for i, user in enumerate(users):
            if i % 100 == 0 and i > 0:
                print(f'  Fetching follower counts... {i}/{len(users)}', file=__import__('sys').stderr)

            followers = self.get_follower_count(user['id'])
            followers_map[user['id']] = followers
            time.sleep(0.1)  # Rate limiting (Twitch allows 800/min for app tokens)

        # Store in database
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        now = datetime.utcnow().isoformat()
        ingested = 0

        for user in users:
            try:
                user_id = user['id']
                stream = stream_map.get(user_id)
                followers = followers_map.get(user_id, 0)

                creator_id = f'twitch_{user_id}'

                c.execute('''INSERT OR REPLACE INTO twitch_creators (
                    id, twitch_id, login, display_name, description, profile_image_url,
                    broadcaster_type, followers, current_viewers, is_live, game_name,
                    stream_title, language, last_seen_live, created_at, updated_at,
                    ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    creator_id,
                    user_id,
                    user.get('login', ''),
                    user.get('display_name', ''),
                    user.get('description', ''),
                    user.get('profile_image_url', ''),
                    user.get('broadcaster_type', ''),
                    followers,
                    stream.get('viewer_count', 0) if stream else 0,
                    1 if stream else 0,
                    stream.get('game_name', '') if stream else '',
                    stream.get('title', '') if stream else '',
                    stream.get('language', '') if stream else '',
                    stream.get('started_at', '') if stream else None,
                    user.get('created_at', now),
                    now,
                    now
                ))
                ingested += 1
            except Exception as e:
                print(f'Error ingesting user {user.get("login")}: {e}', file=__import__('sys').stderr)
                errors += 1

        conn.commit()
        conn.close()

        print(f'Ingestion complete: {ingested} creators stored ({errors} errors)', file=__import__('sys').stderr)
        return {'total_creators': ingested, 'errors': errors}


# Global ingestion status
_twitch_ingestion_status = {
    'status': 'idle',  # idle, running, complete
    'ingested': 0,
    'target': 0,
    'errors': 0,
    'started_at': None,
    'completed_at': None
}
_twitch_ingestion_lock = threading.Lock()


class RequestHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler for Agency Outreach Bot API"""

    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self._set_cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)

        # Clean up query params (parse_qs returns lists)
        params = {k: v[0] if v else '' for k, v in query_params.items()}

        try:
            # Routes that don't require authentication
            if path == '/healthz' or path == '/health':
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok"}).encode())
                return
            elif path == '/login':
                self._serve_login_page()
                return

            # Check authentication for all other routes
            if not self._check_session():
                # Redirect browsers to login page, return JSON for API calls
                if path.startswith('/api/'):
                    self._send_json(401, {'error': 'Unauthorized. Please log in.'})
                else:
                    self.send_response(302)
                    self.send_header('Location', '/login')
                    self.end_headers()
                return

            # Protected routes
            if path == '/':
                self._serve_static('index.html')
            elif path.startswith('/static/'):
                self._serve_static(path[8:])
            elif path == '/api/creators':
                self._handle_get_creators()
            elif path.startswith('/api/creators/'):
                creator_id = path.split('/')[-1]
                self._handle_get_creator(creator_id)
            elif path == '/api/brands':
                self._handle_get_brands(params)
            elif path == '/api/brands/stats':
                self._handle_brands_stats()
            elif path == '/api/campaigns':
                self._handle_get_campaigns(params)
            elif path == '/api/campaigns/pipeline':
                self._handle_campaigns_pipeline()
            elif path == '/api/outreach/templates':
                self._handle_get_templates()
            elif path == '/api/analytics/overview':
                self._handle_analytics_overview()
            elif path == '/api/analytics/activity':
                limit = int(params.get('limit', '50'))
                self._handle_activity_feed(limit)
            elif path == '/api/settings':
                self._handle_get_settings()
            elif path == '/api/scout/search':
                self._handle_scout_search(params)
            elif path == '/api/stats/daily':
                self._handle_get_daily_stats(params)
            elif path == '/api/stats/summary':
                self._handle_get_stats_summary(params)
            elif path == '/api/stats/refresh':
                force = params.get('force', 'false').lower() == 'true'
                self._handle_stats_refresh(force)
            elif path.startswith('/api/stats/live/'):
                creator_id = path.split('/')[-1]
                self._handle_stats_live(creator_id)
            elif path.startswith('/api/ratecard/'):
                creator_id = path.split('/')[-1]
                self._handle_ratecard(creator_id)
            elif path == '/api/platform_status':
                self._handle_platform_status()
            elif path == '/api/twitch/ingest':
                self._handle_twitch_ingest(params)
            elif path == '/api/twitch/ingest/status':
                self._handle_twitch_ingest_status()
            elif path == '/api/twitch/search':
                self._handle_twitch_search(params)
            elif path == '/api/twitch/stats':
                self._handle_twitch_stats()
            elif path.startswith('/api/twitch/creator/'):
                twitch_id = path.split('/')[-1]
                self._handle_twitch_creator(twitch_id)
            else:
                self._send_json(404, {'error': 'Not found'})
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    def do_POST(self):
        """Handle POST requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path

        # Login endpoints don't require session
        if path == '/api/login':
            self._handle_login()
            return
        elif path == '/api/logout':
            self._handle_logout()
            return

        # Check authentication for all other routes
        if not self._check_session():
            self._send_json(401, {'error': 'Unauthorized. Please log in.'})
            return

        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')
        data = json.loads(body) if body else {}

        try:
            if path == '/api/brands':
                self._handle_create_brand(data)
            elif path == '/api/brands/upload':
                self._handle_upload_brands(content_length)
            elif path == '/api/campaigns':
                self._handle_create_campaign(data)
            elif path == '/api/outreach/compose':
                self._handle_compose_message(data)
            elif path == '/api/outreach/send-email':
                self._handle_send_email(data)
            elif path == '/api/outreach/log':
                self._handle_log_outreach(data)
            elif path == '/api/settings':
                self._handle_save_settings(data)
            elif path == '/api/creators':
                self._handle_create_creator(data)
            elif path == '/api/scout/shortlist':
                self._handle_shortlist_creator(data)
            elif path == '/api/stats/daily':
                self._handle_record_daily_stats(data)
            else:
                self._send_json(404, {'error': 'Not found'})
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    def do_PUT(self):
        """Handle PUT requests"""
        # Check authentication
        if not self._check_session():
            self._send_json(401, {'error': 'Unauthorized. Please log in.'})
            return

        parsed_path = urlparse(self.path)
        path = parsed_path.path
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')
        data = json.loads(body) if body else {}

        try:
            if path.startswith('/api/brands/'):
                brand_id = path.split('/')[-1]
                self._handle_update_brand(brand_id, data)
            elif path.startswith('/api/campaigns/'):
                campaign_id = path.split('/')[-1]
                self._handle_update_campaign(campaign_id, data)
            elif path.startswith('/api/creators/'):
                creator_id = path.split('/')[-1]
                self._handle_update_creator(creator_id, data)
            else:
                self._send_json(404, {'error': 'Not found'})
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    def do_DELETE(self):
        """Handle DELETE requests"""
        # Check authentication
        if not self._check_session():
            self._send_json(401, {'error': 'Unauthorized. Please log in.'})
            return

        path = urlparse(self.path).path

        try:
            if path.startswith('/api/brands/'):
                brand_id = path.split('/')[-1]
                self._handle_delete_brand(brand_id)
            elif path.startswith('/api/creators/'):
                creator_id = path.split('/')[-1]
                self._handle_delete_creator(creator_id)
            else:
                self._send_json(404, {'error': 'Not found'})
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    # ============ Authentication Methods ============

    def _check_session(self) -> bool:
        """Check if request has valid session cookie. If no password set, always allow."""
        if not APP_PASSWORD:
            return True
        cookie_header = self.headers.get('Cookie', '')
        if 'session_id=' in cookie_header:
            session_id = cookie_header.split('session_id=')[1].split(';')[0].strip()
            return session_id in VALID_SESSIONS
        return False

    def _serve_login_page(self):
        """Serve the login page with RezTheGiant branding"""
        html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RezTheGiant — Outreach Portal</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: #0a0a0f;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            color: #f0f0f5;
        }
        .bg-grid {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background-image: linear-gradient(rgba(0,212,255,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(0,212,255,0.03) 1px, transparent 1px);
            background-size: 60px 60px; pointer-events: none;
        }
        .glow-1 { position: fixed; top: -200px; right: -100px; width: 500px; height: 500px; border-radius: 50%; background: #00d4ff; filter: blur(150px); opacity: 0.12; pointer-events: none; }
        .glow-2 { position: fixed; bottom: -200px; left: -100px; width: 500px; height: 500px; border-radius: 50%; background: #a855f7; filter: blur(150px); opacity: 0.12; pointer-events: none; }
        .container { width: 100%; max-width: 420px; padding: 20px; position: relative; z-index: 1; }
        .card {
            background: #1a1a2e;
            border: 1px solid #2a2a3e;
            border-radius: 20px;
            padding: 3rem;
            text-align: center;
        }
        .lock { font-size: 2.5rem; margin-bottom: 1.25rem; display: block; }
        .brand { font-family: 'JetBrains Mono', monospace; font-size: 1.1rem; font-weight: 700; margin-bottom: 0.5rem; }
        .brand em { color: #00d4ff; font-style: normal; }
        .badge { display: inline-block; padding: 0.15rem 0.5rem; background: rgba(0,255,136,0.1); border: 1px solid rgba(0,255,136,0.2); border-radius: 4px; font-size: 0.65rem; font-family: 'JetBrains Mono', monospace; font-weight: 600; color: #00ff88; text-transform: uppercase; letter-spacing: 1px; vertical-align: super; margin-left: 4px; }
        .subtitle { color: #8888a0; margin-bottom: 2rem; font-size: 0.85rem; line-height: 1.5; }
        .form-group { margin-bottom: 1.25rem; text-align: left; }
        label { display: block; margin-bottom: 0.5rem; font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; font-weight: 500; color: #8888a0; text-transform: uppercase; letter-spacing: 1.5px; }
        input[type="password"] {
            width: 100%; padding: 0.85rem 1rem;
            background: rgba(255,255,255,0.04); border: 1px solid #2a2a3e; border-radius: 10px;
            color: #f0f0f5; font-family: 'Inter', sans-serif; font-size: 1rem;
            outline: none; transition: all 0.3s;
        }
        input[type="password"]:focus { border-color: #00d4ff; box-shadow: 0 0 20px rgba(0,212,255,0.15); }
        input[type="password"].shake { border-color: #ff4444; box-shadow: 0 0 20px rgba(255,68,68,0.15); animation: shake 0.4s ease; }
        @keyframes shake { 0%,100%{transform:translateX(0)} 25%{transform:translateX(-8px)} 50%{transform:translateX(8px)} 75%{transform:translateX(-4px)} }
        button {
            width: 100%; padding: 0.85rem;
            background: linear-gradient(135deg, #00d4ff, #0088cc);
            color: #000; font-family: 'Inter', sans-serif; font-size: 0.95rem; font-weight: 700;
            border: none; border-radius: 10px; cursor: pointer;
            transition: all 0.3s; box-shadow: 0 0 20px rgba(0,212,255,0.3);
        }
        button:hover { transform: translateY(-2px); box-shadow: 0 0 40px rgba(0,212,255,0.4); }
        button:active { transform: translateY(0); }
        .error { color: #ff4444; font-size: 0.8rem; margin-top: 1rem; min-height: 1.2rem; font-weight: 500; display: none; }
        .error.show { display: block; }
        .footer { margin-top: 2rem; padding-top: 1.5rem; border-top: 1px solid #2a2a3e; font-size: 0.75rem; color: #8888a0; }
        .footer a { color: #00d4ff; text-decoration: none; }
    </style>
</head>
<body>
    <div class="bg-grid"></div>
    <div class="glow-1"></div>
    <div class="glow-2"></div>
    <div class="container">
        <div class="card">
            <span class="lock">&#x1F512;</span>
            <div class="brand"><em>Rez</em>TheGiant <span class="badge">Outreach</span></div>
            <p class="subtitle">This portal is restricted. Enter the access code to continue.</p>
            <form id="loginForm">
                <div class="form-group">
                    <label for="password">Access Code</label>
                    <input type="password" id="password" name="password" placeholder="Enter password..." required autofocus>
                </div>
                <button type="submit">Authenticate</button>
                <div class="error" id="error"></div>
            </form>
            <div class="footer"><a href="https://rezthegiant.com">rezthegiant.com</a> &mdash; RezTheGiant LLC</div>
        </div>
    </div>
    <script>
        document.getElementById('loginForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const password = document.getElementById('password').value;
            const errorDiv = document.getElementById('error');

            try {
                const response = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ password })
                });

                if (response.ok) {
                    window.location.href = '/';
                } else {
                    errorDiv.textContent = 'Invalid password';
                    errorDiv.classList.add('show');
                }
            } catch (err) {
                errorDiv.textContent = 'Login failed: ' + err.message;
                errorDiv.classList.add('show');
            }
        });
    </script>
</body>
</html>"""
        self.send_response(200)
        self._set_cors_headers()
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())

    def _handle_login(self):
        """POST /api/login - Check password and create session"""
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')
        data = json.loads(body) if body else {}

        password = data.get('password', '')

        if password == APP_PASSWORD:
            session_id = secrets.token_urlsafe(32)
            VALID_SESSIONS.add(session_id)

            self.send_response(200)
            self._set_cors_headers()
            self.send_header('Content-Type', 'application/json')
            self.send_header('Set-Cookie', f'session_id={session_id}; Path=/; HttpOnly; SameSite=Strict')
            self.end_headers()
            self.wfile.write(json.dumps({'success': True}).encode())
        else:
            self._send_json(401, {'error': 'Invalid password'})

    def _handle_logout(self):
        """POST /api/logout - Clear session"""
        cookie_header = self.headers.get('Cookie', '')
        if 'session_id=' in cookie_header:
            session_id = cookie_header.split('session_id=')[1].split(';')[0].strip()
            VALID_SESSIONS.discard(session_id)

        self.send_response(200)
        self._set_cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.send_header('Set-Cookie', 'session_id=; Path=/; Max-Age=0')
        self.end_headers()
        self.wfile.write(json.dumps({'success': True}).encode())

    # ============ Helper Methods ============

    def _set_cors_headers(self):
        """Set CORS headers"""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')

    def _send_json(self, status_code: int, data: Dict[str, Any]):
        """Send JSON response"""
        self.send_response(status_code)
        self._set_cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        response = json.dumps(data)
        self.wfile.write(response.encode('utf-8'))

    def _serve_static(self, filename: str):
        """Serve static files"""
        static_dir = BASE_DIR / 'static'
        if filename == '':
            filename = 'index.html'

        file_path = static_dir / filename
        if file_path.exists() and file_path.is_file():
            with open(file_path, 'rb') as f:
                content = f.read()

            self.send_response(200)
            self._set_cors_headers()

            # Determine content type
            if filename.endswith('.html'):
                self.send_header('Content-Type', 'text/html')
            elif filename.endswith('.css'):
                self.send_header('Content-Type', 'text/css')
            elif filename.endswith('.js'):
                self.send_header('Content-Type', 'application/javascript')
            else:
                self.send_header('Content-Type', 'application/octet-stream')

            self.end_headers()
            self.wfile.write(content)
        else:
            self._send_json(404, {'error': 'File not found'})

    # ============ Database Methods ============

    def _init_db(self):
        """Initialize database with tables and seed data"""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Create tables
        c.execute('''CREATE TABLE IF NOT EXISTS brands (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            vertical TEXT,
            contact_name TEXT,
            contact_email TEXT,
            contact_title TEXT,
            website TEXT,
            instagram TEXT,
            linkedin TEXT,
            tiktok TEXT,
            budget_tier TEXT,
            outreach_status TEXT DEFAULT 'New',
            notes TEXT,
            created_at TEXT,
            updated_at TEXT
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS campaigns (
            id TEXT PRIMARY KEY,
            brand_id TEXT NOT NULL,
            creator_id TEXT NOT NULL,
            creator_name TEXT,
            brand_name TEXT,
            channel TEXT,
            status TEXT DEFAULT 'Draft',
            subject TEXT,
            body TEXT,
            value REAL DEFAULT 0,
            notes TEXT,
            created_at TEXT,
            last_activity TEXT
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS outreach_log (
            id TEXT PRIMARY KEY,
            campaign_id TEXT,
            brand_id TEXT,
            creator_id TEXT,
            channel TEXT,
            recipient TEXT,
            subject TEXT,
            body TEXT,
            status TEXT,
            sent_at TEXT,
            replied_at TEXT,
            notes TEXT
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS activity_log (
            id TEXT PRIMARY KEY,
            activity_type TEXT,
            message TEXT,
            creator_id TEXT,
            brand_id TEXT,
            campaign_id TEXT,
            created_at TEXT
        )''')

        # Create creators table
        c.execute('''CREATE TABLE IF NOT EXISTS creators (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            niche TEXT,
            bio TEXT,
            platforms TEXT,
            services TEXT,
            pricing_tier TEXT,
            audience TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            rate_card_url TEXT,
            verticals TEXT,
            brand_partnerships TEXT,
            recent_posts TEXT,
            created_at TEXT,
            updated_at TEXT
        )''')

        # Create daily_stats table
        c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
            id TEXT PRIMARY KEY,
            creator_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            date TEXT NOT NULL,
            followers INTEGER,
            views INTEGER,
            engagement INTEGER,
            new_followers INTEGER,
            watch_hours INTEGER,
            peak_viewers INTEGER,
            created_at TEXT
        )''')

        # Platform API cache table (stores fetched API responses for 1 hour)
        c.execute('''CREATE TABLE IF NOT EXISTS platform_api_cache (
            id TEXT PRIMARY KEY,
            creator_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            source TEXT DEFAULT 'api'
        )''')

        # Create shortlist table
        c.execute('''CREATE TABLE IF NOT EXISTS shortlist (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            handle TEXT,
            platform TEXT,
            followers INTEGER,
            engagement_rate REAL,
            niche TEXT,
            avatar_placeholder TEXT,
            bio TEXT,
            recent_growth REAL,
            match_score INTEGER,
            added_at TEXT
        )''')

        # Twitch creators table (ingested from Twitch API)
        c.execute('''CREATE TABLE IF NOT EXISTS twitch_creators (
            id TEXT PRIMARY KEY,
            twitch_id TEXT UNIQUE,
            login TEXT,
            display_name TEXT,
            description TEXT,
            profile_image_url TEXT,
            broadcaster_type TEXT,
            followers INTEGER DEFAULT 0,
            total_views INTEGER DEFAULT 0,
            current_viewers INTEGER DEFAULT 0,
            is_live INTEGER DEFAULT 0,
            game_name TEXT,
            stream_title TEXT,
            language TEXT,
            avg_viewers INTEGER DEFAULT 0,
            peak_viewers INTEGER DEFAULT 0,
            hours_streamed REAL DEFAULT 0,
            last_seen_live TEXT,
            created_at TEXT,
            updated_at TEXT,
            ingested_at TEXT
        )''')

        conn.commit()

        # Seed brands if table is empty
        c.execute('SELECT COUNT(*) FROM brands')
        if c.fetchone()[0] == 0:
            for brand in SEED_BRANDS:
                brand_id = str(uuid.uuid4())
                now = datetime.utcnow().isoformat()
                c.execute('''INSERT INTO brands VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (brand_id, brand['name'], brand['vertical'], brand['contact_name'],
                     brand['contact_email'], brand['contact_title'], brand['website'],
                     brand['instagram'], brand['linkedin'], brand['tiktok'],
                     brand['budget_tier'], 'New', None, now, now))
            conn.commit()

        # Seed creators if table is empty
        c.execute('SELECT COUNT(*) FROM creators')
        if c.fetchone()[0] == 0:
            for creator_id, creator in CREATORS.items():
                now = datetime.utcnow().isoformat()
                c.execute('''INSERT INTO creators VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (creator['id'], creator['name'], creator['niche'], creator['bio'],
                     json.dumps(creator['platforms']), json.dumps(creator['services']),
                     creator['pricing_tier'], creator['audience'], creator['contact_email'],
                     creator['contact_phone'], creator['rate_card_url'],
                     json.dumps(creator['verticals']), json.dumps(creator.get('brand_partnerships', [])),
                     json.dumps(creator.get('recent_posts', [])), now, now))
            conn.commit()

        # Seed daily_stats if table is empty
        c.execute('SELECT COUNT(*) FROM daily_stats')
        if c.fetchone()[0] == 0:
            self._seed_daily_stats(conn)

        conn.close()

    def _seed_daily_stats(self, conn: sqlite3.Connection):
        """Seed daily_stats with 30 days of realistic mock data for all 3 creators"""
        c = conn.cursor()
        now = datetime.utcnow()

        # Creator stats definitions: (creator_id, creator_name, platform_data)
        # platform_data: {platform: (base_followers, base_views, base_engagement, base_watch_hours)}
        creator_stats = {
            "1": {  # Kalani Rodgers - Comedy
                "name": "Kalani Rodgers",
                "platforms": {
                    "tiktok": (2400000, 450000, 32000, 0),
                    "instagram": (1200000, 120000, 8900, 0),
                    "youtube": (800000, 85000, 5200, 80),
                    "twitter": (250000, 15000, 800, 0),
                    "twitch": (150000, 25000, 3000, 120),
                    "kick": (80000, 12000, 1500, 95),
                }
            },
            "2": {  # Coach Canela - Fitness
                "name": "Coach Canela",
                "platforms": {
                    "tiktok": (720000, 250000, 25000, 0),
                    "instagram": (580000, 180000, 16500, 0),
                    "youtube": (420000, 95000, 8200, 160),
                    "twitter": (180000, 10000, 600, 0),
                    "twitch": (220000, 35000, 3800, 200),
                    "kick": (140000, 18000, 2200, 145),
                }
            },
            "3": {  # Kayla Rae Ortiz - Wellness
                "name": "Kayla Rae Ortiz",
                "platforms": {
                    "tiktok": (580000, 280000, 31500, 0),
                    "instagram": (420000, 140000, 13700, 0),
                    "youtube": (310000, 72000, 6800, 140),
                    "twitter": (120000, 8000, 450, 0),
                    "twitch": (185000, 28000, 3200, 180),
                    "kick": (95000, 14000, 1800, 110),
                }
            }
        }

        for creator_id, creator_data in creator_stats.items():
            for platform, (base_followers, base_views, base_engagement, base_watch_hours) in creator_data["platforms"].items():
                # Generate 30 days of data with realistic fluctuation and slight upward trend
                for day_offset in range(30):
                    date = (now - timedelta(days=29 - day_offset)).strftime('%Y-%m-%d')

                    # Add small random variation and slight upward trend
                    day_variation = (day_offset / 30.0) * 0.05  # 5% growth over 30 days
                    day_noise = (hash(f"{creator_id}{platform}{day_offset}") % 10 - 5) / 100.0  # -5% to +5%

                    followers = int(base_followers * (1 + day_variation + day_noise))
                    views = int(base_views * (1 + day_variation + day_noise * 1.5))
                    engagement = int(base_engagement * (1 + day_variation + day_noise * 1.5))
                    new_followers = int((followers - int(base_followers * (1 + day_variation + day_noise * 0.9))) * 1.1) if day_offset > 0 else int(followers * 0.02)
                    watch_hours = int(base_watch_hours * (1 + day_variation + day_noise)) if base_watch_hours > 0 else 0
                    peak_viewers = int(base_watch_hours * 0.3) if base_watch_hours > 0 else 0

                    stat_id = str(uuid.uuid4())
                    c.execute('''INSERT INTO daily_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (stat_id, creator_id, platform, date, followers, views, engagement,
                         new_followers, watch_hours, peak_viewers, now.isoformat()))

        conn.commit()

    def _get_db(self):
        """Get database connection"""
        return sqlite3.connect(DB_PATH)

    # ============ API Handlers - Creators ============

    def _handle_get_creators(self):
        """GET /api/creators - List all creators from database"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('SELECT * FROM creators ORDER BY name')
        rows = c.fetchall()
        conn.close()

        creators = []
        for row in rows:
            creator = {
                'id': row[0],
                'name': row[1],
                'niche': row[2],
                'bio': row[3],
                'platforms': json.loads(row[4]) if row[4] else {},
                'services': json.loads(row[5]) if row[5] else [],
                'pricing_tier': row[6],
                'audience': row[7],
                'contact_email': row[8],
                'contact_phone': row[9],
                'rate_card_url': row[10],
                'verticals': json.loads(row[11]) if row[11] else [],
                'brand_partnerships': json.loads(row[12]) if row[12] else [],
                'recent_posts': json.loads(row[13]) if row[13] else []
            }
            creators.append(creator)

        self._send_json(200, {'creators': creators})

    def _handle_get_creator(self, creator_id: str):
        """GET /api/creators/:id - Get single creator from database"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('SELECT * FROM creators WHERE id = ?', (creator_id,))
        row = c.fetchone()
        conn.close()

        if row:
            creator = {
                'id': row[0],
                'name': row[1],
                'niche': row[2],
                'bio': row[3],
                'platforms': json.loads(row[4]) if row[4] else {},
                'services': json.loads(row[5]) if row[5] else [],
                'pricing_tier': row[6],
                'audience': row[7],
                'contact_email': row[8],
                'contact_phone': row[9],
                'rate_card_url': row[10],
                'verticals': json.loads(row[11]) if row[11] else [],
                'brand_partnerships': json.loads(row[12]) if row[12] else [],
                'recent_posts': json.loads(row[13]) if row[13] else []
            }
            self._send_json(200, creator)
        else:
            self._send_json(404, {'error': 'Creator not found'})

    # ============ API Handlers - Brands ============

    def _handle_get_brands(self, params: Dict[str, str]):
        """GET /api/brands - List brands with filtering"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        query = 'SELECT * FROM brands WHERE 1=1'
        filter_params = []

        if params.get('vertical'):
            query += ' AND vertical = ?'
            filter_params.append(params['vertical'])

        if params.get('status'):
            query += ' AND outreach_status = ?'
            filter_params.append(params['status'])

        if params.get('search'):
            query += ' AND (name LIKE ? OR contact_email LIKE ?)'
            search_term = f"%{params['search']}%"
            filter_params.extend([search_term, search_term])

        c.execute(query, filter_params)
        brands = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
        conn.close()

        self._send_json(200, {'brands': brands})

    def _handle_brands_stats(self):
        """GET /api/brands/stats - Get brand statistics"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('SELECT outreach_status, COUNT(*) FROM brands GROUP BY outreach_status')
        status_counts = {row[0]: row[1] for row in c.fetchall()}

        c.execute('SELECT vertical, COUNT(*) FROM brands GROUP BY vertical')
        vertical_counts = {row[0]: row[1] for row in c.fetchall()}

        c.execute('SELECT COUNT(*) FROM brands')
        total = c.fetchone()[0]

        conn.close()

        self._send_json(200, {
            'total': total,
            'by_status': status_counts,
            'by_vertical': vertical_counts
        })

    def _handle_create_brand(self, data: Dict[str, Any]):
        """POST /api/brands - Create brand"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        brand_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        c.execute('''INSERT INTO brands VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (brand_id, data.get('name'), data.get('vertical'), data.get('contact_name'),
             data.get('contact_email'), data.get('contact_title'), data.get('website'),
             data.get('instagram'), data.get('linkedin'), data.get('tiktok'),
             data.get('budget_tier'), 'New', data.get('notes'), now, now))
        conn.commit()
        conn.close()

        self._log_activity('brand_created', f"Brand '{data.get('name')}' created", None, brand_id, None)
        self._send_json(201, {'id': brand_id, **data, 'created_at': now})

    def _handle_update_brand(self, brand_id: str, data: Dict[str, Any]):
        """PUT /api/brands/:id - Update brand"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        now = datetime.utcnow().isoformat()
        updates = []
        params = []

        for field in ['name', 'vertical', 'contact_name', 'contact_email', 'contact_title',
                      'website', 'instagram', 'linkedin', 'tiktok', 'budget_tier', 'outreach_status', 'notes']:
            if field in data:
                updates.append(f'{field} = ?')
                params.append(data[field])

        if updates:
            updates.append('updated_at = ?')
            params.append(now)
            params.append(brand_id)

            query = f"UPDATE brands SET {', '.join(updates)} WHERE id = ?"
            c.execute(query, params)
            conn.commit()

        conn.close()
        self._log_activity('brand_updated', f"Brand updated", None, brand_id, None)
        self._send_json(200, {'id': brand_id, **data, 'updated_at': now})

    def _handle_delete_brand(self, brand_id: str):
        """DELETE /api/brands/:id - Delete brand"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('DELETE FROM brands WHERE id = ?', (brand_id,))
        conn.commit()
        conn.close()

        self._send_json(200, {'deleted': True})

    def _handle_upload_brands(self, content_length: int):
        """POST /api/brands/upload - Upload brands from CSV"""
        try:
            # Parse multipart form data
            content_type = self.headers.get('Content-Type', '')

            if 'multipart/form-data' not in content_type:
                self._send_json(400, {'error': 'Expected multipart/form-data'})
                return

            # Extract boundary
            boundary = content_type.split('boundary=')[1].encode() if 'boundary=' in content_type else b''
            body = self.rfile.read(content_length)

            # Simple multipart parser for file extraction
            parts = body.split(b'--' + boundary)
            csv_data = None

            for part in parts:
                if b'filename=' in part and b'text/csv' in part or b'application/vnd.ms-excel' in part:
                    # Extract file content
                    lines = part.split(b'\r\n')
                    # Find the empty line that separates headers from content
                    for i, line in enumerate(lines):
                        if line == b'':
                            csv_data = b'\r\n'.join(lines[i+1:-2])
                            break

            if not csv_data:
                self._send_json(400, {'error': 'No CSV file found in upload'})
                return

            # Parse CSV
            csv_text = csv_data.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(csv_text))

            self._init_db()
            conn = self._get_db()
            c = conn.cursor()

            added = 0
            updated = 0
            now = datetime.utcnow().isoformat()

            for row in csv_reader:
                email = row.get('contact_email', '').strip()
                if not email:
                    continue

                # Check if brand with this email exists
                c.execute('SELECT id FROM brands WHERE contact_email = ?', (email,))
                existing = c.fetchone()

                if existing:
                    # Update existing
                    brand_id = existing[0]
                    c.execute('''UPDATE brands SET name=?, vertical=?, contact_name=?,
                               contact_title=?, website=?, instagram=?, linkedin=?, tiktok=?,
                               budget_tier=?, updated_at=? WHERE id=?''',
                        (row.get('name'), row.get('vertical'), row.get('contact_name'),
                         row.get('contact_title'), row.get('website'), row.get('instagram'),
                         row.get('linkedin'), row.get('tiktok'), row.get('budget_tier'),
                         now, brand_id))
                    updated += 1
                else:
                    # Insert new
                    brand_id = str(uuid.uuid4())
                    c.execute('''INSERT INTO brands VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (brand_id, row.get('name'), row.get('vertical'), row.get('contact_name'),
                         email, row.get('contact_title'), row.get('website'), row.get('instagram'),
                         row.get('linkedin'), row.get('tiktok'), row.get('budget_tier'),
                         'New', None, now, now))
                    added += 1

            conn.commit()
            conn.close()

            self._log_activity('brands_uploaded', f"Uploaded {added} new brands, updated {updated}", None, None, None)
            self._send_json(200, {'added': added, 'updated': updated, 'total': added + updated})
        except Exception as e:
            self._send_json(400, {'error': f'Upload failed: {str(e)}'})

    # ============ API Handlers - Campaigns ============

    def _handle_get_campaigns(self, params: Dict[str, str]):
        """GET /api/campaigns - List campaigns"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        query = 'SELECT * FROM campaigns WHERE 1=1'
        filter_params = []

        if params.get('creator_id'):
            query += ' AND creator_id = ?'
            filter_params.append(params['creator_id'])

        if params.get('status'):
            query += ' AND status = ?'
            filter_params.append(params['status'])

        c.execute(query, filter_params)
        campaigns = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
        conn.close()

        self._send_json(200, {'campaigns': campaigns})

    def _handle_campaigns_pipeline(self):
        """GET /api/campaigns/pipeline - Kanban pipeline stats"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('SELECT status, COUNT(*) FROM campaigns GROUP BY status')
        pipeline = {row[0]: row[1] for row in c.fetchall()}

        conn.close()
        self._send_json(200, pipeline)

    def _handle_create_campaign(self, data: Dict[str, Any]):
        """POST /api/campaigns - Create campaign"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        # Get creator and brand names
        creator = CREATORS.get(data.get('creator_id'), {})
        creator_name = creator.get('name', '')

        c.execute('SELECT name FROM brands WHERE id = ?', (data.get('brand_id'),))
        row = c.fetchone()
        brand_name = row[0] if row else ''

        campaign_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        c.execute('''INSERT INTO campaigns VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (campaign_id, data.get('brand_id'), data.get('creator_id'), creator_name,
             brand_name, data.get('channel'), 'Draft', data.get('subject'),
             data.get('body'), data.get('value', 0), data.get('notes'), now, now))
        conn.commit()
        conn.close()

        self._log_activity('campaign_created', f"Campaign created: {creator_name} → {brand_name}",
                          data.get('creator_id'), data.get('brand_id'), campaign_id)
        self._send_json(201, {'id': campaign_id, **data, 'created_at': now})

    def _handle_update_campaign(self, campaign_id: str, data: Dict[str, Any]):
        """PUT /api/campaigns/:id - Update campaign"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        now = datetime.utcnow().isoformat()
        updates = []
        params = []

        for field in ['status', 'subject', 'body', 'value', 'notes', 'channel']:
            if field in data:
                updates.append(f'{field} = ?')
                params.append(data[field])

        if updates:
            updates.append('last_activity = ?')
            params.append(now)
            params.append(campaign_id)

            query = f"UPDATE campaigns SET {', '.join(updates)} WHERE id = ?"
            c.execute(query, params)
            conn.commit()

        conn.close()
        self._log_activity('campaign_updated', f"Campaign updated: status={data.get('status')}",
                          None, None, campaign_id)
        self._send_json(200, {'id': campaign_id, **data})

    # ============ API Handlers - Outreach ============

    def _handle_get_templates(self):
        """GET /api/outreach/templates - Get all templates"""
        organized = {}
        for channel, templates in TEMPLATES.items():
            organized[channel] = templates

        self._send_json(200, organized)

    def _handle_compose_message(self, data: Dict[str, Any]):
        """POST /api/outreach/compose - Compose message from template"""
        template_id = data.get('template_id')
        creator_id = data.get('creator_id')
        brand_id = data.get('brand_id')

        creator = CREATORS.get(creator_id, {})

        self._init_db()
        conn = self._get_db()
        c = conn.cursor()
        c.execute('SELECT * FROM brands WHERE id = ?', (brand_id,))
        brand_row = c.fetchone()
        conn.close()

        if not brand_row:
            self._send_json(404, {'error': 'Brand not found'})
            return

        brand_cols = [col[0] for col in c.description] if c.description else []
        brand = dict(zip(brand_cols, brand_row)) if brand_row else {}

        # Find template
        template = None
        for channel_templates in TEMPLATES.values():
            for t in channel_templates:
                if t['id'] == template_id:
                    template = t
                    break

        if not template:
            self._send_json(404, {'error': 'Template not found'})
            return

        # Compose message by replacing placeholders
        placeholders = {
            'creator_name': creator.get('name', ''),
            'creator_niche': creator.get('niche', ''),
            'total_reach': f"{creator.get('total_reach', 0):,}",
            'avg_engagement_rate': f"{creator.get('avg_engagement_rate', 0):.1f}",
            'audience': creator.get('audience', ''),
            'niche': creator.get('niche', ''),
            'brand_name': brand.get('name', ''),
            'contact_name': brand.get('contact_name', ''),
            'vertical': brand.get('vertical', '')
        }

        subject = template.get('subject', '')
        body = template.get('body', '')

        for key, value in placeholders.items():
            subject = subject.replace(f'{{{{{key}}}}}', str(value))
            body = body.replace(f'{{{{{key}}}}}', str(value))

        self._send_json(200, {
            'subject': subject,
            'body': body,
            'template_id': template_id,
            'channel': template.get('channel')
        })

    def _handle_send_email(self, data: Dict[str, Any]):
        """POST /api/outreach/send-email - Actually send email"""
        to_email = data.get('to')
        subject = data.get('subject')
        body = data.get('body')
        creator_id = data.get('creator_id')
        brand_id = data.get('brand_id')
        campaign_id = data.get('campaign_id')

        try:
            # Send email via SMTP
            send_email(to_email, subject, body)

            # Log the outreach
            self._init_db()
            log_id = str(uuid.uuid4())
            now = datetime.utcnow().isoformat()

            conn = self._get_db()
            c = conn.cursor()
            c.execute('''INSERT INTO outreach_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (log_id, campaign_id, brand_id, creator_id, 'email', to_email,
                 subject, body, 'sent', now, None, None))

            # Update campaign status if needed
            if campaign_id:
                c.execute('UPDATE campaigns SET status = ? WHERE id = ?', ('Sent', campaign_id))

            # Update brand outreach status
            if brand_id:
                c.execute('UPDATE brands SET outreach_status = ? WHERE id = ?', ('Outreached', brand_id))

            conn.commit()
            conn.close()

            self._log_activity('email_sent', f"Email sent to {to_email}",
                             creator_id, brand_id, campaign_id)

            self._send_json(200, {'sent': True, 'id': log_id})
        except Exception as e:
            self._send_json(500, {'error': f'Failed to send email: {str(e)}'})

    def _handle_log_outreach(self, data: Dict[str, Any]):
        """POST /api/outreach/log - Log manual outreach"""
        self._init_db()
        log_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        conn = self._get_db()
        c = conn.cursor()
        c.execute('''INSERT INTO outreach_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (log_id, data.get('campaign_id'), data.get('brand_id'), data.get('creator_id'),
             data.get('channel'), data.get('recipient'), data.get('subject', ''),
             data.get('body', ''), 'sent', now, None, None))

        if data.get('brand_id'):
            c.execute('UPDATE brands SET outreach_status = ? WHERE id = ?',
                     ('Outreached', data.get('brand_id')))

        conn.commit()
        conn.close()

        self._log_activity('outreach_logged', f"{data.get('channel')} outreach logged",
                          data.get('creator_id'), data.get('brand_id'), data.get('campaign_id'))

        self._send_json(201, {'id': log_id})

    # ============ API Handlers - Analytics ============

    def _handle_analytics_overview(self):
        """GET /api/analytics/overview - Analytics dashboard"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('SELECT COUNT(*) FROM outreach_log')
        total_outreach = c.fetchone()[0]

        c.execute('SELECT COUNT(*) FROM outreach_log WHERE replied_at IS NOT NULL')
        replies = c.fetchone()[0]

        c.execute('SELECT SUM(value) FROM campaigns WHERE status = ?', ('Closed Won',))
        pipeline_value = c.fetchone()[0] or 0

        c.execute('SELECT COUNT(*) FROM campaigns WHERE status = ?', ('Closed Won',))
        deals_closed = c.fetchone()[0]

        conn.close()

        response_rate = (replies / total_outreach * 100) if total_outreach > 0 else 0

        self._send_json(200, {
            'total_outreach': total_outreach,
            'response_rate': round(response_rate, 1),
            'pipeline_value': pipeline_value,
            'deals_closed': deals_closed
        })

    def _handle_activity_feed(self, limit: int):
        """GET /api/analytics/activity - Recent activity feed"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('SELECT * FROM activity_log ORDER BY created_at DESC LIMIT ?', (limit,))
        activities = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
        conn.close()

        self._send_json(200, {'activities': activities})

    # ============ API Handlers - Settings ============

    def _handle_get_settings(self):
        """GET /api/settings - Get current settings"""
        if Path(SETTINGS_PATH).exists():
            with open(SETTINGS_PATH, 'r') as f:
                settings = json.load(f)
        else:
            settings = {
                'company_name': 'Rez Agency',
                'email': SMTP_FROM,
                'reply_to': SMTP_FROM,
                'signature': 'Rez | Talent Manager'
            }

        self._send_json(200, settings)

    def _handle_save_settings(self, data: Dict[str, Any]):
        """POST /api/settings - Save settings"""
        with open(SETTINGS_PATH, 'w') as f:
            json.dump(data, f, indent=2)

        self._send_json(200, {'saved': True})

    # ============ API Handlers - Creator Management ============

    def _handle_create_creator(self, data: Dict[str, Any]):
        """POST /api/creators - Add a new creator"""
        creator_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''INSERT INTO creators VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (creator_id, data.get('name'), data.get('niche'), data.get('bio'),
             json.dumps(data.get('platforms', {})), json.dumps(data.get('services', [])),
             data.get('pricing_tier'), data.get('audience'), data.get('contact_email'),
             data.get('contact_phone'), data.get('rate_card_url'),
             json.dumps(data.get('verticals', [])), json.dumps(data.get('brand_partnerships', [])),
             json.dumps(data.get('recent_posts', [])), now, now))

        conn.commit()
        conn.close()

        self._log_activity('creator_added', f"Creator '{data.get('name')}' added", creator_id)

        self._send_json(201, {'id': creator_id, 'created': True})

    def _handle_update_creator(self, creator_id: str, data: Dict[str, Any]):
        """PUT /api/creators/<id> - Update an existing creator"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        now = datetime.utcnow().isoformat()

        # Build update query dynamically based on provided fields
        update_fields = []
        update_values = []

        if 'name' in data:
            update_fields.append('name = ?')
            update_values.append(data['name'])
        if 'niche' in data:
            update_fields.append('niche = ?')
            update_values.append(data['niche'])
        if 'bio' in data:
            update_fields.append('bio = ?')
            update_values.append(data['bio'])
        if 'platforms' in data:
            update_fields.append('platforms = ?')
            update_values.append(json.dumps(data['platforms']))
        if 'services' in data:
            update_fields.append('services = ?')
            update_values.append(json.dumps(data['services']))
        if 'pricing_tier' in data:
            update_fields.append('pricing_tier = ?')
            update_values.append(data['pricing_tier'])
        if 'audience' in data:
            update_fields.append('audience = ?')
            update_values.append(data['audience'])
        if 'contact_email' in data:
            update_fields.append('contact_email = ?')
            update_values.append(data['contact_email'])
        if 'contact_phone' in data:
            update_fields.append('contact_phone = ?')
            update_values.append(data['contact_phone'])
        if 'rate_card_url' in data:
            update_fields.append('rate_card_url = ?')
            update_values.append(data['rate_card_url'])
        if 'verticals' in data:
            update_fields.append('verticals = ?')
            update_values.append(json.dumps(data['verticals']))
        if 'brand_partnerships' in data:
            update_fields.append('brand_partnerships = ?')
            update_values.append(json.dumps(data['brand_partnerships']))
        if 'recent_posts' in data:
            update_fields.append('recent_posts = ?')
            update_values.append(json.dumps(data['recent_posts']))

        if update_fields:
            update_fields.append('updated_at = ?')
            update_values.append(now)
            update_values.append(creator_id)

            query = f"UPDATE creators SET {', '.join(update_fields)} WHERE id = ?"
            c.execute(query, update_values)
            conn.commit()

        conn.close()

        self._log_activity('creator_updated', f"Creator '{creator_id}' updated", creator_id)

        self._send_json(200, {'id': creator_id, 'updated': True})

    def _handle_delete_creator(self, creator_id: str):
        """DELETE /api/creators/<id> - Delete a creator"""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('DELETE FROM creators WHERE id = ?', (creator_id,))
        conn.commit()
        conn.close()

        self._log_activity('creator_deleted', f"Creator '{creator_id}' deleted", creator_id)

        self._send_json(200, {'id': creator_id, 'deleted': True})

    # ============ API Handlers - Scout Creators ============

    def _handle_scout_search(self, params: Dict[str, str]):
        """GET /api/scout/search - Search for potential creators"""
        query = params.get('query', '')
        platform = params.get('platform', '')
        niche = params.get('niche', '')
        min_followers = int(params.get('min_followers', 0))
        max_followers = int(params.get('max_followers', 999999999))

        # Generate realistic mock scout results
        scout_results = self._generate_scout_results(query, platform, niche, min_followers, max_followers)

        self._send_json(200, {'results': scout_results, 'count': len(scout_results)})

    def _generate_scout_results(self, query: str, platform: str, niche: str, min_followers: int, max_followers: int) -> List[Dict[str, Any]]:
        """Generate realistic mock scout results based on search parameters"""
        creator_names = [
            "Alex Chen", "Jordan Martinez", "Casey Williams", "Morgan Lee", "Taylor Brooks",
            "Riley Davis", "Sam Anderson", "Parker Smith", "Casey Johnson", "Jamie Taylor",
            "Morgan Garcia", "Alex Rodriguez", "Jordan Harris", "Taylor Thomas", "Sam Moore",
            "Riley Jackson", "Casey White", "Jordan Harris", "Alex Martin", "Taylor Thompson"
        ]

        platforms_list = ["tiktok", "youtube", "instagram", "twitch", "kick", "x"]
        niches_list = ["Comedy", "Fitness", "Wellness", "Gaming", "Beauty", "Tech", "Lifestyle", "Education"]

        results = []
        hash_seed = hash(f"{query}{platform}{niche}")

        for i in range(15):
            idx = (hash_seed + i) % len(creator_names)
            handle = f"@{creator_names[idx].lower().replace(' ', '_')}{i}"

            selected_platform = platform if platform else platforms_list[(hash_seed + i) % len(platforms_list)]
            selected_niche = niche if niche else niches_list[(hash_seed + i) % len(niches_list)]

            # Generate followers within the requested range
            base_followers = min_followers + ((hash_seed + i) % (max_followers - min_followers))
            followers = max(min_followers, min(base_followers, max_followers))

            engagement_rate = 5.0 + ((hash_seed + i) % 80) / 10.0
            recent_growth = ((hash_seed + i) % 25) - 5

            match_score = 50 + ((hash_seed + i) % 50)
            if query.lower() in selected_niche.lower():
                match_score = min(100, match_score + 15)

            results.append({
                'name': creator_names[idx],
                'handle': handle,
                'platform': selected_platform,
                'followers': followers,
                'engagement_rate': round(engagement_rate, 1),
                'niche': selected_niche,
                'avatar_placeholder': f'https://api.dicebear.com/7.x/avataaars/svg?seed={handle}',
                'bio': f"{selected_niche} content creator with {followers:,} followers",
                'recent_growth': recent_growth,
                'match_score': match_score
            })

        return sorted(results, key=lambda x: x['match_score'], reverse=True)

    def _handle_shortlist_creator(self, data: Dict[str, Any]):
        """POST /api/scout/shortlist - Save a scouted creator to shortlist"""
        self._init_db()
        shortlist_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        conn = self._get_db()
        c = conn.cursor()

        c.execute('''INSERT INTO shortlist VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (shortlist_id, data.get('name'), data.get('handle'), data.get('platform'),
             data.get('followers'), data.get('engagement_rate'), data.get('niche'),
             data.get('avatar_placeholder'), data.get('bio'), data.get('recent_growth'),
             data.get('match_score'), now))

        conn.commit()
        conn.close()

        self._send_json(201, {'id': shortlist_id, 'shortlisted': True})

    # ============ API Handlers - Daily Stats ============

    def _handle_get_daily_stats(self, params: Dict[str, str]):
        """GET /api/stats/daily - Get daily platform stats"""
        creator_id = params.get('creator_id')
        platform = params.get('platform')
        date_from = params.get('date_from')
        date_to = params.get('date_to')

        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        query = 'SELECT * FROM daily_stats WHERE 1=1'
        query_params = []

        if creator_id:
            query += ' AND creator_id = ?'
            query_params.append(creator_id)
        if platform:
            query += ' AND platform = ?'
            query_params.append(platform)
        if date_from:
            query += ' AND date >= ?'
            query_params.append(date_from)
        if date_to:
            query += ' AND date <= ?'
            query_params.append(date_to)

        query += ' ORDER BY date DESC'
        c.execute(query, query_params)
        rows = c.fetchall()
        conn.close()

        stats = []
        for row in rows:
            stats.append({
                'id': row[0],
                'creator_id': row[1],
                'platform': row[2],
                'date': row[3],
                'followers': row[4],
                'views': row[5],
                'engagement': row[6],
                'new_followers': row[7],
                'watch_hours': row[8],
                'peak_viewers': row[9]
            })

        self._send_json(200, {'stats': stats, 'count': len(stats)})

    def _handle_record_daily_stats(self, data: Dict[str, Any]):
        """POST /api/stats/daily - Record daily stats"""
        self._init_db()
        stat_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        conn = self._get_db()
        c = conn.cursor()

        c.execute('''INSERT INTO daily_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (stat_id, data.get('creator_id'), data.get('platform'), data.get('date'),
             data.get('followers'), data.get('views'), data.get('engagement'),
             data.get('new_followers'), data.get('watch_hours'), data.get('peak_viewers'), now))

        conn.commit()
        conn.close()

        self._send_json(201, {'id': stat_id, 'recorded': True})

    def _handle_get_stats_summary(self, params: Dict[str, str]):
        """GET /api/stats/summary - Get aggregated stats summary"""
        creator_id = params.get('creator_id')

        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        # Get the latest stats across all platforms for the creator
        if creator_id:
            c.execute('''
                SELECT platform, followers, views, engagement, new_followers, date
                FROM daily_stats
                WHERE creator_id = ?
                ORDER BY date DESC
                LIMIT 6
            ''', (creator_id,))
        else:
            c.execute('''
                SELECT platform, followers, views, engagement, new_followers, date
                FROM daily_stats
                ORDER BY date DESC
                LIMIT 18
            ''')

        rows = c.fetchall()

        # Calculate aggregates
        total_followers = 0
        platform_stats = {}
        best_platform = None
        best_engagement = 0
        dates = []

        for row in rows:
            platform, followers, views, engagement, new_followers, date = row
            if platform not in platform_stats:
                platform_stats[platform] = {'followers': followers, 'engagement': engagement}
            total_followers += followers if followers else 0
            if engagement and engagement > best_engagement:
                best_engagement = engagement
                best_platform = platform
            if date not in dates:
                dates.append(date)

        # Calculate daily growth rate (7-day trend)
        daily_growth_rate = 0.0
        if len(dates) >= 2:
            first_date = dates[-1]
            last_date = dates[0]
            days_diff = max(1, len(dates) - 1)
            daily_growth_rate = ((len(dates) - 1) / days_diff) if days_diff > 0 else 0

        summary = {
            'total_followers': total_followers,
            'daily_growth_rate': round(daily_growth_rate, 2),
            'best_performing_platform': best_platform,
            'platform_stats': platform_stats,
            'trend_period_days': len(dates)
        }

        conn.close()

        self._send_json(200, summary)

    # ============ Platform API Handlers ============

    def _handle_stats_refresh(self, force: bool = False):
        """GET /api/stats/refresh — Trigger fresh pull from all platform APIs."""
        self._init_db()
        api = get_platform_api()
        results = {}
        errors = {}
        for creator_id in CREATORS:
            try:
                stats = api.fetch_all(creator_id, force=force)
                results[creator_id] = {
                    'creator_name': CREATORS[creator_id]['name'],
                    'platforms_fetched': list(stats.keys()),
                    'sources': {p: s.get('source', 'unknown') for p, s in stats.items()},
                }
                # Write latest stats snapshot to daily_stats table
                today = datetime.utcnow().strftime('%Y-%m-%d')
                conn = self._get_db()
                c = conn.cursor()
                for platform, s in stats.items():
                    stat_id = str(uuid.uuid4())
                    now = datetime.utcnow().isoformat()
                    followers = s.get('followers', 0)
                    views = s.get('total_views', s.get('views', 0))
                    engagement = int(followers * (s.get('engagement_rate', 0) / 100)) if s.get('engagement_rate') else 0
                    c.execute(
                        'INSERT OR REPLACE INTO daily_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (stat_id, creator_id, platform, today,
                         followers, views, engagement, 0, 0, 0, now)
                    )
                conn.commit()
                conn.close()
            except Exception as ex:
                errors[creator_id] = str(ex)

        self._send_json(200, {
            'refreshed': True,
            'forced': force,
            'results': results,
            'errors': errors,
            'timestamp': datetime.utcnow().isoformat(),
        })

    def _handle_stats_live(self, creator_id: str):
        """GET /api/stats/live/<creator_id> — Latest cached stats for one creator."""
        self._init_db()
        if creator_id not in CREATORS:
            self._send_json(404, {'error': 'Creator not found'})
            return
        api = get_platform_api()
        all_stats = api.fetch_all(creator_id)

        total_followers = sum(s.get('followers', 0) for s in all_stats.values())
        eng_rates = [
            s.get('engagement_rate') or CREATORS[creator_id]['platforms'].get(p, {}).get('engagement_rate', 0)
            for p, s in all_stats.items()
            if s.get('followers', 0) > 0
        ]
        avg_engagement = round(sum(eng_rates) / len(eng_rates), 2) if eng_rates else 0

        self._send_json(200, {
            'creator_id': creator_id,
            'creator_name': CREATORS[creator_id]['name'],
            'total_followers': total_followers,
            'avg_engagement_rate': avg_engagement,
            'platforms': all_stats,
            'fetched_at': datetime.utcnow().isoformat(),
        })

    def _handle_ratecard(self, creator_id: str):
        """GET /api/ratecard/<creator_id> — Generate rate card from real metrics."""
        self._init_db()
        if creator_id not in CREATORS:
            self._send_json(404, {'error': 'Creator not found'})
            return
        api = get_platform_api()
        rate_card = api.generate_rate_card(creator_id)
        self._send_json(200, rate_card)

    def _handle_platform_status(self):
        """GET /api/platform_status — Show which API keys are configured."""
        self._send_json(200, {
            'youtube': {
                'configured': bool(YOUTUBE_API_KEY),
                'note': 'Set YOUTUBE_API_KEY env var (Google Cloud Console)',
                'free_tier': '10,000 units/day',
            },
            'twitch': {
                'configured': bool(TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET),
                'note': 'Set TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET (dev.twitch.tv)',
                'free_tier': 'Free with client credentials',
            },
            'kick': {
                'configured': True,
                'note': 'Uses public API endpoint — no key required',
                'free_tier': 'Public (no auth needed)',
            },
            'instagram': {
                'configured': bool(INSTAGRAM_ACCESS_TOKEN),
                'note': 'Set INSTAGRAM_ACCESS_TOKEN (Meta Developer App + Business Account)',
                'free_tier': 'Free with Meta Developer account',
            },
            'x': {
                'configured': bool(X_BEARER_TOKEN),
                'note': 'Set X_BEARER_TOKEN — requires X API Basic tier ($100/mo)',
                'free_tier': 'Basic tier required for read access',
            },
            'tiktok': {
                'configured': bool(TIKTOK_ACCESS_TOKEN),
                'note': 'Set TIKTOK_ACCESS_TOKEN — requires Research API approval at developers.tiktok.com',
                'free_tier': 'Free but requires application approval',
            },
        })

    # ============ Twitch Ingestion & Search ============

    def _handle_twitch_ingest(self, params: Dict[str, str]):
        """GET /api/twitch/ingest — Trigger full ingestion of top Twitch streamers.
        Uses threading so it doesn't block. Returns immediately with status.
        """
        if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
            self._send_json(400, {'error': 'Twitch API credentials not configured'})
            return

        target_count = int(params.get('count', '2000'))

        # Check if already running
        with _twitch_ingestion_lock:
            if _twitch_ingestion_status['status'] == 'running':
                self._send_json(200, {
                    'status': 'already_running',
                    'message': 'Ingestion already in progress',
                    'started_at': _twitch_ingestion_status['started_at']
                })
                return

            # Mark as running
            _twitch_ingestion_status['status'] = 'running'
            _twitch_ingestion_status['target'] = target_count
            _twitch_ingestion_status['ingested'] = 0
            _twitch_ingestion_status['errors'] = 0
            _twitch_ingestion_status['started_at'] = datetime.utcnow().isoformat()
            _twitch_ingestion_status['completed_at'] = None

        # Start ingestion in background thread
        def _ingest_thread():
            try:
                twitch_api = TwitchAPI(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
                result = twitch_api.ingest_top_streamers(target_count)
                with _twitch_ingestion_lock:
                    _twitch_ingestion_status['status'] = 'complete'
                    _twitch_ingestion_status['ingested'] = result.get('total_creators', 0)
                    _twitch_ingestion_status['errors'] = result.get('errors', 0)
                    _twitch_ingestion_status['completed_at'] = datetime.utcnow().isoformat()
            except Exception as e:
                print(f'Twitch ingestion error: {e}', file=__import__('sys').stderr)
                with _twitch_ingestion_lock:
                    _twitch_ingestion_status['status'] = 'error'
                    _twitch_ingestion_status['errors'] += 1
                    _twitch_ingestion_status['completed_at'] = datetime.utcnow().isoformat()

        thread = threading.Thread(target=_ingest_thread, daemon=True)
        thread.start()

        self._send_json(200, {
            'status': 'ingestion_started',
            'target_count': target_count,
            'started_at': _twitch_ingestion_status['started_at']
        })

    def _handle_twitch_ingest_status(self):
        """GET /api/twitch/ingest/status — Check ingestion progress."""
        with _twitch_ingestion_lock:
            self._send_json(200, {
                'status': _twitch_ingestion_status['status'],
                'ingested': _twitch_ingestion_status['ingested'],
                'target': _twitch_ingestion_status['target'],
                'errors': _twitch_ingestion_status['errors'],
                'started_at': _twitch_ingestion_status['started_at'],
                'completed_at': _twitch_ingestion_status['completed_at']
            })

    def _handle_twitch_search(self, params: Dict[str, str]):
        """GET /api/twitch/search — Search local database of ingested Twitch creators.
        Query params: q, min_followers, max_followers, min_viewers, game, language,
        sort_by (followers/viewers/avg_viewers), order (asc/desc), page, per_page.
        """
        query = params.get('q', '').strip()
        min_followers = int(params.get('min_followers', '0'))
        max_followers = int(params.get('max_followers', '999999999'))
        min_viewers = int(params.get('min_viewers', '0'))
        game = params.get('game', '').strip()
        language = params.get('language', '').strip()
        sort_by = params.get('sort_by', 'followers')  # followers, viewers, avg_viewers
        order = params.get('order', 'desc').upper()  # ASC or DESC
        page = int(params.get('page', '1'))
        per_page = int(params.get('per_page', '50'))

        if order not in ('ASC', 'DESC'):
            order = 'DESC'

        if sort_by not in ('followers', 'current_viewers', 'avg_viewers'):
            sort_by = 'followers'

        # Build WHERE clause
        where_parts = ['1=1']
        params_list = []

        if query:
            where_parts.append('(display_name LIKE ? OR login LIKE ?)')
            q_pattern = f'%{query}%'
            params_list.extend([q_pattern, q_pattern])

        if min_followers > 0:
            where_parts.append('followers >= ?')
            params_list.append(min_followers)

        if max_followers < 999999999:
            where_parts.append('followers <= ?')
            params_list.append(max_followers)

        if min_viewers > 0:
            where_parts.append('current_viewers >= ?')
            params_list.append(min_viewers)

        if game:
            where_parts.append('game_name LIKE ?')
            params_list.append(f'%{game}%')

        if language:
            where_parts.append('language = ?')
            params_list.append(language)

        where_clause = ' AND '.join(where_parts)

        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        # Get total count
        c.execute(f'SELECT COUNT(*) FROM twitch_creators WHERE {where_clause}', params_list)
        total = c.fetchone()[0]

        # Get paginated results
        offset = (page - 1) * per_page
        query_sql = f'''SELECT * FROM twitch_creators
                        WHERE {where_clause}
                        ORDER BY {sort_by} {order}
                        LIMIT ? OFFSET ?'''
        params_list.extend([per_page, offset])

        c.execute(query_sql, params_list)
        rows = c.fetchall()
        cols = [desc[0] for desc in c.description]
        results = [dict(zip(cols, row)) for row in rows]

        conn.close()

        self._send_json(200, {
            'results': results,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        })

    def _handle_twitch_stats(self):
        """GET /api/twitch/stats — Summary stats for ingested Twitch creators."""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        # Total creators ingested
        c.execute('SELECT COUNT(*) FROM twitch_creators')
        total_creators = c.fetchone()[0]

        # Average followers
        c.execute('SELECT AVG(followers) FROM twitch_creators')
        avg_followers = c.fetchone()[0] or 0

        # Top games (by streamer count)
        c.execute('''SELECT game_name, COUNT(*) as count FROM twitch_creators
                     WHERE game_name != '' AND game_name IS NOT NULL
                     GROUP BY game_name ORDER BY count DESC LIMIT 10''')
        top_games = [{'game': row[0], 'count': row[1]} for row in c.fetchall()]

        # Total live streamers
        c.execute('SELECT COUNT(*) FROM twitch_creators WHERE is_live = 1')
        total_live = c.fetchone()[0]

        # Last ingestion time
        c.execute('SELECT MAX(ingested_at) FROM twitch_creators')
        last_ingestion = c.fetchone()[0]

        conn.close()

        self._send_json(200, {
            'total_creators_ingested': total_creators,
            'avg_followers': int(avg_followers),
            'top_games': top_games,
            'total_live': total_live,
            'last_ingestion_time': last_ingestion
        })

    def _handle_twitch_creator(self, twitch_id: str):
        """GET /api/twitch/creator/<twitch_id> — Get full details for a specific creator."""
        self._init_db()
        conn = self._get_db()
        c = conn.cursor()

        c.execute('SELECT * FROM twitch_creators WHERE twitch_id = ?', (twitch_id,))
        row = c.fetchone()
        conn.close()

        if not row:
            self._send_json(404, {'error': 'Creator not found'})
            return

        cols = [desc[0] for desc in c.description]
        creator = dict(zip(cols, row))
        self._send_json(200, creator)

    # ============ Helper Methods ============

    def _log_activity(self, activity_type: str, message: str, creator_id: Optional[str] = None,
                      brand_id: Optional[str] = None, campaign_id: Optional[str] = None):
        """Log activity to activity log"""
        self._init_db()
        activity_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        conn = self._get_db()
        c = conn.cursor()
        c.execute('''INSERT INTO activity_log VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (activity_id, activity_type, message, creator_id, brand_id, campaign_id, now))
        conn.commit()
        conn.close()

    def log_message(self, format, *args):
        """Override to suppress default logging"""
        pass


def send_email(to_email: str, subject: str, body_html: str, from_name: str = "Rez | Talent Manager") -> bool:
    """Send email via Google Workspace SMTP"""
    if not SMTP_USER or not SMTP_PASS or not SMTP_FROM:
        raise Exception("SMTP credentials not configured")

    msg = MIMEMultipart('alternative')
    msg['From'] = f'{from_name} <{SMTP_FROM}>'
    msg['To'] = to_email
    msg['Subject'] = subject
    msg['Reply-To'] = SMTP_FROM

    # Plain text version
    text_body = body_html.replace('<br>', '\n').replace('</p>', '\n')
    text_part = MIMEText(text_body, 'plain')

    # HTML version with styling
    html_body = f'''<html><body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
{body_html}
<br><br>
<p style="color: #666; font-size: 12px;">—<br>Rez | Talent Manager<br>{SMTP_FROM}</p>
</body></html>'''
    html_part = MIMEText(html_body, 'html')

    msg.attach(text_part)
    msg.attach(html_part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

    return True


def warmup_server():
    """Make a request to warm up the server (for Render cold starts)"""
    try:
        time.sleep(1)  # Give server time to start
        req = urllib.request.Request('http://localhost:' + str(PORT) + '/healthz')
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # Silently fail if warmup doesn't work


def run_server():
    """Start the HTTP server"""
    server_address = ('0.0.0.0', PORT)
    httpd = HTTPServer(server_address, RequestHandler)
    print(f'Server running on port {PORT}')

    # Start warmup in background thread
    warmup_thread = threading.Thread(target=warmup_server, daemon=True)
    warmup_thread.start()

    httpd.serve_forever()


if __name__ == '__main__':
    run_server()
