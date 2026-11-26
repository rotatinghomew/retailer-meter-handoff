"""HTTP 服务入口兼容模块。"""

from service import Api, SwitchService, build_http_server, main  # noqa: F401

if __name__ == "__main__":
    main()
