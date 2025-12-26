
# Confessions Bot

A powerful, open-source, and anonymous confessions bot for Telegram, designed to build and moderate a safe community space.

![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg) ![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

---

Welcome to the Telegram Confessions Bot, a comprehensive solution for creating an engaging, anonymous platform for any community. This bot provides a full suite of features for users, authors, and administrators, ensuring a seamless and safe experience from submission to moderation.

<!-- A great addition here would be a GIF showing the user flow: /confess -> write -> admin review -> channel post -> user commenting -->
 


## Key Features

This bot is packed with features designed for robust community management and user engagement.

#### For Communities & Users
*   **Anonymous Submissions:** Users can share their thoughts without revealing their identity.
*   **Interactive Commenting:** Full-featured comment sections with text, stickers, GIFs, and replies.
*   **Aura Points System:** Build anonymous reputation through a points system for likes/dislikes on comments.
*   **Personal Profiles:** Users can track their own submission history and Aura points.

#### For Administrators
*   **Approval Workflow:** Review and approve every confession before it's posted to the channel.
*   **Reasoned Rejections:** Inform users why their submission was not approved.
*   **Full User Management:** A complete set of commands to `/warn`, `/block` (temporarily), `/pblock` (permanently), and `/unblock` users.
*   **User Insights:** Get a detailed overview of any user's activity with the `/id` command.

<br>

## Full Documentation & Live Demo

For a complete step-by-step setup guide, a deep-dive into every feature, a live showcase of bots using this code, and interactive examples, please visit the official documentation page.

### [**View the Full Documentation Page**](https://addisuderrese.github.io/confessions-bot/)

The documentation page provides a much richer, more detailed, and interactive experience for getting your bot up and running.

<br>

## Quick Installation

For experienced users, here is the quick setup process. For detailed explanations of each step, please **refer to the [documentation page](https://addisuderrese.github.io/confessions-bot/#setup)**.

**1. Clone the repository:**
```bash
git clone https://github.com/addisuderrese/confessions-bot.git
cd confessions-bot 
```

**2. Install dependencies:**

```
pip install -r requirements.txt
```

**3. Configure your environment:**

-   Create a file named .env in the root directory.
    
-   Add your credentials (BOT_TOKEN, ADMIN_ID, CHANNEL_ID, DATABASE_URL).
    
-   See the "Configuration" section in the documentation for detailed instructions on where to get these values.
    

**4. Run the bot:**


```
python main.py
```

## Contributing

Contributions are welcome! This project is open-source, and we encourage you to help improve it. If you have ideas for new features or have found a bug, please feel free to fork the repository and open a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](https://www.google.com/url?sa=E&q=LICENSE) file for details.