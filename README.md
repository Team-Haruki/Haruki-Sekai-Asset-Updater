# Haruki Sekai Asset Updater

A game asset extractor with HTTP API for Project Sekai

## How to use

+ Install [uv](https://github.com/astral-sh/uv) at first
+ Install dependencies by following command:  
`uv sync`
+ Copy configs.example.py as a new file configs.py, then configure it.  
+ Run it with command  
`uv run main.py`
+ For test, you can use the following example and try to start running updater:  
```
POST http://127.0.0.1:12345/update_asset  
{  
    "server": "jp",  
    "assetVersion": "5.1.0.60",  
    "assetHash": "74e0f354-81b4-32aa-1d45-a7cec042ea43"  
}
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright © 2025 Haruki Dev Team