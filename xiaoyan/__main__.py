"""
xiaoyan.__main__ — python -m xiaoyan 入口
============================================
支持:
  python -m xiaoyan          # 启动服务
  python -m xiaoyan init     # 交互式配置
"""

from xiaoyan.cli import main

if __name__ == "__main__":
    main()
