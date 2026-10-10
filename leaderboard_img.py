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
