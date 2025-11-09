# Haruki Sekai Asset Updater

**Haruki Sekai Asset Updater** is a companion project for [HarukiBot](https://github.com/Team-Haruki), designed to export game assets and provide a HTTP API.

## Requirements
+ `AssetStudioModCLI` modified version (click [here]("https://github.com/Team-Haruki/AssetStudio"))  
+ `ffmpeg` (For converting audio files and video files)
+ `cwebp` (Optional, for converting image files to webp format)

## How to Use
1. Go to release page to download `HarukiSekaiAssetUpdater` executable file.
2. Rename `haruki-asset-configs.example.yaml` to `haruki-asset-configs.yaml` and then edit it. For more details, see the `haruki-asset-configs.example.yaml` comments.
3. Make a new directory or use an exists directory
4. Put `HarukiSekaiAssetUpdater` and `haruki-asset-configs.yaml` in the same directory
5. Open Terminal, and `cd` to the directory
6. Run `HarukiSekaiAssetUpdater`

## License

This project is licensed under the MIT License.