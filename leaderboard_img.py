import os
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
from pilmoji import Pilmoji

def generate_leaderboard_image(top_users):
    # Image dimensions
    width = 800
    row_height = 80
    header_height = 120
    padding = 40
    
    # Check if empty
    if not top_users:
        top_users = [{"profile_name": "No users yet", "points": 0}]
        
    height = header_height + (len(top_users) * row_height) + padding
    
    # Colors
    bg_color = (25, 25, 35)       # Dark background
    card_color = (35, 35, 50)     # Slightly lighter row
    text_primary = (255, 255, 255)
    text_secondary = (180, 180, 200)
    accent_color = (120, 100, 255) # Purple accent
    
    # Create image
    img = Image.new('RGB', (width, height), bg_color)
    draw = ImageDraw.Draw(img)
    
    # Load fonts
    font_path = "Poppins-SemiBold.ttf"
    try:
        title_font = ImageFont.truetype(font_path, 40)
        name_font = ImageFont.truetype(font_path, 32)
        points_font = ImageFont.truetype(font_path, 28)
    except IOError:
        # Fallback
        title_font = ImageFont.load_default()
        name_font = ImageFont.load_default()
        points_font = ImageFont.load_default()

    # Draw header
    with Pilmoji(img) as pilmoji:
        # Centered title
        from datetime import datetime
        current_month = datetime.now().strftime("%B")
        title_text = f"🏆 Top Aura Holders - {current_month} 🏆"
        try:
            bbox = title_font.getbbox(title_text)
            title_w = bbox[2] - bbox[0]
        except AttributeError:
            title_w = title_font.getlength(title_text)
            
        title_x = (width - title_w) // 2
        pilmoji.text((title_x, padding), title_text, fill=text_primary, font=title_font)
        
        # Draw horizontal line
        draw.line([(padding, header_height), (width - padding, header_height)], fill=accent_color, width=3)
        
        # Draw users
        y_offset = header_height + 20
        medals = ["🥇", "🥈", "🥉"]
        
        for idx, user in enumerate(top_users, 1):
            # Row background
            row_y = y_offset
            draw.rounded_rectangle([(padding, row_y), (width - padding, row_y + row_height - 10)], radius=10, fill=card_color)
            
            # Rank/Medal
            rank_text = medals[idx-1] if idx <= 3 else f"#{idx}"
            pilmoji.text((padding + 20, row_y + 15), rank_text, fill=text_primary, font=name_font)
            
            # Name
            name = user['profile_name']
            if len(name) > 18:
                name = name[:16] + "..."
            pilmoji.text((padding + 90, row_y + 15), name, fill=text_primary, font=name_font)
            
            # Points
            pts_this_month = user.get('points_this_month', 0)
            if pts_this_month > 0:
                points_text = f"⭐ {user['points']} (+{pts_this_month})"
            else:
                points_text = f"⭐ {user['points']} pts"
                
            try:
                bbox = points_font.getbbox(points_text)
                points_w = bbox[2] - bbox[0]
            except AttributeError:
                points_w = points_font.getlength(points_text)
                
            # If they have + points, make the text slightly greener or just use text_secondary
            if pts_this_month > 0:
                pilmoji.text((width - padding - points_w - 20, row_y + 18), points_text, fill=(150, 255, 150), font=points_font)
            else:
                pilmoji.text((width - padding - points_w - 20, row_y + 18), points_text, fill=text_secondary, font=points_font)
            
            y_offset += row_height

    bio = BytesIO()
    img.save(bio, format='PNG')
    bio.seek(0)
    return bio


def generate_giveaway_image(top_confessions):
    width = 850
    row_height = 90
    header_height = 130
    padding = 40
    
    if not top_confessions:
        top_confessions = [{"id": 0, "profile_name": "No entries yet", "giveaway_points": 0, "reaction_count": 0, "comment_count": 0}]
        
    height = header_height + (len(top_confessions) * row_height) + padding
    
    bg_color = (25, 25, 35)       # Dark background
    card_color = (35, 35, 50)     # Row card
    text_primary = (255, 255, 255)
    text_secondary = (180, 180, 200)
    gold_accent = (255, 195, 50)   # Gold accent for giveaway
    
    img = Image.new('RGB', (width, height), bg_color)
    draw = ImageDraw.Draw(img)
    
    font_path = "Poppins-SemiBold.ttf"
    try:
        title_font = ImageFont.truetype(font_path, 38)
        name_font = ImageFont.truetype(font_path, 28)
        sub_font = ImageFont.truetype(font_path, 20)
        pts_font = ImageFont.truetype(font_path, 26)
    except IOError:
        title_font = ImageFont.load_default()
        name_font = ImageFont.load_default()
        sub_font = ImageFont.load_default()
        pts_font = ImageFont.load_default()

    with Pilmoji(img) as pilmoji:
        title_text = "🎁 Giveaway Leaderboard 🏆"
        try:
            bbox = title_font.getbbox(title_text)
            title_w = bbox[2] - bbox[0]
        except AttributeError:
            title_w = title_font.getlength(title_text)
            
        title_x = (width - title_w) // 2
        pilmoji.text((title_x, padding), title_text, fill=(255, 215, 0), font=title_font)
        draw.line([(padding, header_height), (width - padding, header_height)], fill=gold_accent, width=3)
        
        medals = ["🥇", "🥈", "🥉"]
        y_offset = header_height + 15
        
        for idx, item in enumerate(top_confessions, 1):
            row_y = y_offset
            draw.rounded_rectangle([(padding, row_y), (width - padding, row_y + row_height - 12)], radius=12, fill=card_color)
            
            rank_text = medals[idx-1] if idx <= 3 else f"#{idx}"
            pilmoji.text((padding + 18, row_y + 20), rank_text, fill=text_primary, font=name_font)
            
            cid = item.get('id', 0)
            pname = item.get('profile_name', 'Anonymous')
            if cid > 0:
                title_line = f"Confession #{cid} - {pname}"
            else:
                title_line = pname
                
            if len(title_line) > 28:
                title_line = title_line[:26] + "..."
            pilmoji.text((padding + 85, row_y + 14), title_line, fill=text_primary, font=name_font)
            
            rcnt = item.get('reaction_count', 0)
            ccnt = item.get('comment_count', 0)
            details_line = f"❤️ {rcnt} likes   💬 {ccnt} comments (x2)"
            pilmoji.text((padding + 85, row_y + 50), details_line, fill=text_secondary, font=sub_font)
            
            pts = item.get('giveaway_points', 0)
            pts_text = f"🏆 {pts} pts"
            try:
                bbox_pts = pts_font.getbbox(pts_text)
                pts_w = bbox_pts[2] - bbox_pts[0]
            except AttributeError:
                pts_w = pts_font.getlength(pts_text)
                
            pilmoji.text((width - padding - pts_w - 20, row_y + 26), pts_text, fill=(255, 215, 0), font=pts_font)
            
            y_offset += row_height

    bio = BytesIO()
    img.save(bio, format='PNG')
    bio.seek(0)
    return bio
