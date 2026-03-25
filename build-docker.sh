#!/bin/bash
# 构建 Docker 镜像的辅助脚本

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 获取版本号（如果有 git tag）
VERSION=${1:-$(git describe --tags --always --dirty 2>/dev/null || echo "dev")}

echo -e "${GREEN}=== 开始构建 Haruki Sekai Asset Updater Docker 镜像 ===${NC}"
echo -e "${YELLOW}版本: ${VERSION}${NC}"

# 构建镜像
echo -e "\n${GREEN}[1/3] 构建 Docker 镜像...${NC}"
docker build \
  --build-arg VERSION="${VERSION}" \
  -t haruki-sekai-asset-updater:${VERSION} \
  -t haruki-sekai-asset-updater:latest \
  .

echo -e "\n${GREEN}[2/3] 构建完成！${NC}"

# 显示镜像信息
echo -e "\n${GREEN}[3/3] 镜像信息:${NC}"
docker images haruki-sekai-asset-updater --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"

echo -e "\n${GREEN}=== 构建完成 ===${NC}"
echo -e "\n${YELLOW}运行镜像:${NC}"
echo "docker run -d -p 8080:8080 \\"
echo "  -v \$(pwd)/haruki-asset-configs.yaml:/app/haruki-asset-configs.yaml:ro \\"
echo "  -v \$(pwd)/Data:/app/Data \\"
echo "  -v \$(pwd)/logs:/app/logs \\"
echo "  --name haruki-updater \\"
echo "  haruki-sekai-asset-updater:latest"

echo -e "\n${YELLOW}查看镜像层:${NC}"
echo "docker history haruki-sekai-asset-updater:latest"
