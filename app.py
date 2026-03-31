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
                self._send_json(401, {'error': 'Unauthorized. Please log in.'})
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
        """Serve the login page with dark theme"""
        html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Agency Outreach Bot - Login</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #0f0f1e 0%, #1a1a2e 100%);
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            color: #fff;
        }
        .container {
            width: 100%;
            max-width: 400px;
            padding: 20px;
        }
        .card {
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border-radius: 12px;
            padding: 40px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        }
        h1 {
            margin-bottom: 10px;
            font-size: 28px;
            text-align: center;
        }
        .subtitle {
            text-align: center;
            color: rgba(255, 255, 255, 0.6);
            margin-bottom: 30px;
            font-size: 14px;
        }
        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            margin-bottom: 8px;
            font-size: 14px;
            font-weight: 500;
            color: rgba(255, 255, 255, 0.9);
        }
        input[type="password"] {
            width: 100%;
            padding: 12px 15px;
            border: 1px solid rgba(255, 255, 255, 0.15);
            border-radius: 8px;
            background: rgba(255, 255, 255, 0.08);
            color: #fff;
            font-size: 14px;
            transition: all 0.3s ease;
        }
        input[type="password"]:focus {
            outline: none;
            background: rgba(255, 255, 255, 0.12);
            border-color: rgba(255, 255, 255, 0.25);
            box-shadow: 0 0 0 3px rgba(255, 255, 255, 0.05);
        }
        button {
            width: 100%;
            padding: 12px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #fff;
            border: none;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            margin-top: 10px;
        }
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 24px rgba(102, 126, 234, 0.4);
        }
        button:active {
            transform: translateY(0);
        }
        .error {
            color: #ff6b6b;
            font-size: 13px;
            margin-top: 10px;
            display: none;
        }
        .error.show {
            display: block;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <h1>Agency Bot</h1>
            <p class="subtitle">Enter password to continue</p>
            <form id="loginForm">
                <div class="form-group">
                    <label for="password">Password</label>
                    <input type="password" id="password" name="password" placeholder="Enter password" required autofocus>
                </div>
                <button type="submit">Login</button>
                <div class="error" id="error"></div>
            </form>
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
