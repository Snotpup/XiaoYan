from xiaoyan.scraper.__main__ import build_parser


def _parse(argv):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.show_browser = (
        getattr(args, "global_show_browser", False)
        or getattr(args, "command_show_browser", False)
    )
    return args


def test_global_show_browser_applies_to_subcommand():
    args = _parse(["--show-browser", "wos", "--query", "TS=(AI)"])

    assert args.show_browser is True


def test_command_show_browser_applies_to_subcommand():
    args = _parse(["wos", "--show-browser", "--query", "TS=(AI)"])

    assert args.show_browser is True


def test_wos_defaults_to_com_site():
    args = _parse(["wos", "--query", "TS=(AI)"])

    assert args.site == "com"
    assert args.cn_site is False

