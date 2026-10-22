import asyncio
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import cn2an
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.subscribe import SubscribeChain
from app.plugins import _PluginBase
from app.schemas.types import MediaSource, MediaType
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.media import MediaInfo, MetaInfo, build_media_key, resolve_media_identity
from app.sdk.network import RequestUtils


class PopularSubscribe(_PluginBase):
    """
    热门媒体订阅：按热度自动订阅电影、电视剧、动漫。
    数据来源于 MoviePilot 服务端的订阅统计接口，需在系统设置中开启订阅统计共享；
    同时支持爱奇艺 / 腾讯视频 / 芒果TV 最新电视剧订阅（复用平台探索插件拉取片库），
    以及 Netflix 新剧、韩剧订阅（直接走 TMDB discover，只取已开播且首播不早于上月1日的剧集）。
    """

    # 插件名称
    plugin_name = "热门媒体订阅"
    # 插件描述
    plugin_desc = "自动添加热门电影、电视剧、动漫到订阅；支持订阅爱奇艺/腾讯视频/芒果TV最新电视剧。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/thsrite/MoviePilot-Plugins/main/icons/popular.png"
    # 插件版本
    plugin_version = "3.3.1"
    # 插件作者
    plugin_author = "thsrite"
    # 作者主页
    author_url = "https://github.com/thsrite"
    # 插件配置项ID前缀
    plugin_config_prefix = "popularsubscribe_"
    # 加载顺序
    plugin_order = 25
    # 可使用的用户级别
    auth_level = 2

    # 电影类型
    _TYPE_MOVIE = "电影"
    # 电视剧类型
    _TYPE_TV = "电视剧"
    # 动漫类型
    _TYPE_ANIME = "动漫"
    # MoviePilot Server 的订阅统计接口是外部服务协议，不属于宿主内部模块。
    _SUBSCRIBE_STATISTIC_PATH = "/subscribe/statistic"

    # 电影开关
    _movie_enabled: bool = False
    # 电视剧开关
    _tv_enabled: bool = False
    # 动漫开关
    _anime_enabled: bool = False
    # 获取条数
    _movie_page_cnt: int = 30
    _tv_page_cnt: int = 30
    _anime_page_cnt: int = 30
    # 订阅人次门槛
    _movie_popular_cnt: int = 0
    _tv_popular_cnt: int = 0
    _anime_popular_cnt: int = 0
    # cron 表达式
    _movie_cron: str = ""
    _tv_cron: str = ""
    _anime_cron: str = ""
    # 一次性开关与历史清理
    _onlyonce: bool = False
    _clear: bool = False
    _clear_already_handle: bool = False
    # 订阅用户名
    _username: Optional[str] = None

    # 平台数据源定义：key 为平台标识，value 为展示名与详情页链接模板
    _PLATFORMS: Dict[str, Dict[str, str]] = {
        "iqiyi": {
            "name": "爱奇艺",
            "detail_link": "https://www.iqiyi.com/v_{media_id}.html",
        },
        "tencent": {
            "name": "腾讯视频",
            "detail_link": "https://v.qq.com/x/cover/{media_id}.html",
        },
        "mangguo": {
            "name": "芒果TV",
            "detail_link": "https://www.mgtv.com/b/{media_id}/",
        },
        "netflix": {
            "name": "Netflix",
            "detail_link": "https://www.themoviedb.org/tv/{media_id}",
        },
        "korean": {
            "name": "韩剧",
            "detail_link": "https://www.themoviedb.org/tv/{media_id}",
        },
        "western": {
            "name": "欧美剧",
            "detail_link": "https://www.themoviedb.org/tv/{media_id}",
        },
    }
    # 直接走 TMDB discover 的数据源（无需额外的探索插件）
    _TMDB_SOURCES: Tuple[str, ...] = ("netflix", "korean", "western")
    # 欧美剧取这些出品国
    _TMDB_WESTERN_COUNTRIES = "US|GB|CA|AU|NZ|IE"
    # Netflix 在 TMDB 的网络 ID
    _TMDB_NETFLIX_NETWORK = "213"
    # TMDB 热度门槛：评分人数 / 平均分（低于门槛视为冷门条，不订阅）
    _TMDB_MIN_VOTES = "5"
    _TMDB_MIN_RATING = "6.0"
    # 平台开关（爱奇艺 / 腾讯视频 / 芒果TV）
    _iqiyi_enabled: bool = False
    _tencent_enabled: bool = False
    _mangguo_enabled: bool = False
    # 平台 cron 表达式
    _iqiyi_cron: str = ""
    _tencent_cron: str = ""
    _mangguo_cron: str = ""
    # 平台获取条数
    _iqiyi_page_cnt: int = 10
    _tencent_page_cnt: int = 10
    _mangguo_page_cnt: int = 10
    # Netflix / 韩剧（数据源为 TMDB discover）
    _netflix_enabled: bool = False
    _korean_enabled: bool = False
    _netflix_cron: str = ""
    _korean_cron: str = ""
    _netflix_page_cnt: int = 20
    _korean_page_cnt: int = 20
    # 欧美剧（数据源为 TMDB discover）
    _western_enabled: bool = False
    _western_cron: str = ""
    _western_page_cnt: int = 20
    # 平台订阅过滤：只订上个月以后首播的新剧 / 只订能识别到 TMDB 的剧（过滤短剧）
    _platform_recent_only: bool = True
    _platform_require_tmdb: bool = True
    # 新剧过滤起始日期（上月1日），运行时计算
    _recent_since: Any = None

    # 依赖组件
    downloadchain: Optional[DownloadChain] = None
    subscribechain: Optional[SubscribeChain] = None
    mediachain: Optional[MediaChain] = None
    # 一次性任务调度器
    _scheduler: Optional[BackgroundScheduler] = None
    # 保护 history / already_handle 并发读改写
    _history_lock: threading.Lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        """
        生效插件配置：加载参数、处理清理动作、按需触发立即运行。
        """
        self.downloadchain = DownloadChain()
        self.subscribechain = SubscribeChain()
        self.mediachain = MediaChain()
        # 停止已有的一次性调度器
        self.stop_service()
        self.__migrate_history_identity()

        config = config or {}
        self._movie_enabled = bool(config.get("movie_enabled"))
        self._tv_enabled = bool(config.get("tv_enabled"))
        self._anime_enabled = bool(config.get("anime_enabled"))
        self._movie_cron = config.get("movie_cron") or ""
        self._tv_cron = config.get("tv_cron") or ""
        self._anime_cron = config.get("anime_cron") or ""
        self._movie_page_cnt = self.__to_int(config.get("movie_page_cnt"), 30)
        self._tv_page_cnt = self.__to_int(config.get("tv_page_cnt"), 30)
        self._anime_page_cnt = self.__to_int(config.get("anime_page_cnt"), 30)
        self._movie_popular_cnt = self.__to_int(config.get("movie_popular_cnt"), 0)
        self._tv_popular_cnt = self.__to_int(config.get("tv_popular_cnt"), 0)
        self._anime_popular_cnt = self.__to_int(config.get("anime_popular_cnt"), 0)
        self._clear = bool(config.get("clear"))
        self._clear_already_handle = bool(config.get("clear_already_handle"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._username = (config.get("username") or "").strip() or "热门订阅"
        self._iqiyi_enabled = bool(config.get("iqiyi_enabled"))
        self._tencent_enabled = bool(config.get("tencent_enabled"))
        self._mangguo_enabled = bool(config.get("mangguo_enabled"))
        self._iqiyi_cron = config.get("iqiyi_cron") or ""
        self._tencent_cron = config.get("tencent_cron") or ""
        self._mangguo_cron = config.get("mangguo_cron") or ""
        self._iqiyi_page_cnt = self.__to_int(config.get("iqiyi_page_cnt"), 10)
        self._tencent_page_cnt = self.__to_int(config.get("tencent_page_cnt"), 10)
        self._mangguo_page_cnt = self.__to_int(config.get("mangguo_page_cnt"), 10)
        self._netflix_enabled = bool(config.get("netflix_enabled"))
        self._korean_enabled = bool(config.get("korean_enabled"))
        self._netflix_cron = config.get("netflix_cron") or ""
        self._korean_cron = config.get("korean_cron") or ""
        self._netflix_page_cnt = self.__to_int(config.get("netflix_page_cnt"), 20)
        self._korean_page_cnt = self.__to_int(config.get("korean_page_cnt"), 20)
        self._western_enabled = bool(config.get("western_enabled"))
        self._western_cron = config.get("western_cron") or ""
        self._western_page_cnt = self.__to_int(config.get("western_page_cnt"), 20)
        self._platform_recent_only = bool(config.get("platform_recent_only", True))
        self._platform_require_tmdb = bool(config.get("platform_require_tmdb", True))
        # 新剧过滤起始日期：上个月1日（每次运行时重新计算，随时间自动前移）
        try:
            today = datetime.now().date()
            first_of_month = today.replace(day=1)
            self._recent_since = (first_of_month - timedelta(days=1)).replace(day=1)
        except Exception:
            self._recent_since = None

        if not config:
            return

        # 清理订阅历史
        if self._clear:
            self.del_data(key="history")
            self._clear = False
            logger.info("热门订阅历史清理完成")

        # 清理已处理历史
        if self._clear_already_handle:
            self.del_data(key="already_handle")
            self._clear_already_handle = False
            logger.info("热门订阅已处理记录清理完成")

        # 立即运行一次
        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            run_date = datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3)

            # 三种类型合并为一个作业顺序执行，避免并发写入 history 相互覆盖
            self._scheduler.add_job(
                self.__run_all_enabled,
                trigger="date",
                run_date=run_date,
                name="热门订阅（立即运行一次）",
            )

            self._onlyonce = False
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

        # 持久化关闭一次性开关与清理标记
        self.__update_config()

    def get_state(self) -> bool:
        """
        插件是否启用：任意一个媒体类型开启即视为启用。
        """
        return self._movie_enabled or self._tv_enabled or self._anime_enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        本插件不注册远程命令。
        """
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """
        注册删除历史记录的 API。
        """
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "删除订阅历史记录",
                "response_model": schemas.Response[None],
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册周期性订阅任务，由 MoviePilot 服务框架统一调度。
        """
        services: List[Dict[str, Any]] = []
        if self._movie_enabled and self._movie_cron:
            service = self.__build_service(
                sid="PopularSubscribeMovie",
                name="电影热门订阅",
                cron=self._movie_cron,
                stype=self._TYPE_MOVIE,
                page_cnt=self._movie_page_cnt,
                popular_cnt=self._movie_popular_cnt,
            )
            if service:
                services.append(service)
        if self._tv_enabled and self._tv_cron:
            service = self.__build_service(
                sid="PopularSubscribeTv",
                name="电视剧热门订阅",
                cron=self._tv_cron,
                stype=self._TYPE_TV,
                page_cnt=self._tv_page_cnt,
                popular_cnt=self._tv_popular_cnt,
            )
            if service:
                services.append(service)
        if self._anime_enabled and self._anime_cron:
            service = self.__build_service(
                sid="PopularSubscribeAnime",
                name="动漫热门订阅",
                cron=self._anime_cron,
                stype=self._TYPE_ANIME,
                page_cnt=self._anime_page_cnt,
                popular_cnt=self._anime_popular_cnt,
            )
            if service:
                services.append(service)
        platform_services = self.__build_platform_services()
        if platform_services:
            services.extend(platform_services)
        return services

    def __build_platform_services(self) -> List[Dict[str, Any]]:
        """
        构造三个视频平台的定时任务定义，cron 解析失败会向系统消息推送告警。
        """
        services: List[Dict[str, Any]] = []
        platform_settings = (
            ("PopularSubscribeIqiyi", "爱奇艺最新电视剧订阅", "iqiyi",
             self._iqiyi_enabled, self._iqiyi_cron, self._iqiyi_page_cnt),
            ("PopularSubscribeTencent", "腾讯视频最新电视剧订阅", "tencent",
             self._tencent_enabled, self._tencent_cron, self._tencent_page_cnt),
            ("PopularSubscribeMangguo", "芒果TV最新电视剧订阅", "mangguo",
             self._mangguo_enabled, self._mangguo_cron, self._mangguo_page_cnt),
            ("PopularSubscribeNetflix", "Netflix新剧订阅", "netflix",
             self._netflix_enabled, self._netflix_cron, self._netflix_page_cnt),
            ("PopularSubscribeKorean", "韩剧新剧订阅", "korean",
             self._korean_enabled, self._korean_cron, self._korean_page_cnt),
            ("PopularSubscribeWestern", "欧美剧新剧订阅", "western",
             self._western_enabled, self._western_cron, self._western_page_cnt),
        )
        for sid, name, platform, enabled, cron, page_cnt in platform_settings:
            if not (enabled and cron):
                continue
            try:
                trigger = CronTrigger.from_crontab(cron)
            except Exception as err:
                logger.error(f"{name}定时任务配置错误：{err}")
                self.systemmessage.put(f"{name}执行周期配置错误：{err}")
                continue
            services.append({
                "id": sid,
                "name": name,
                "trigger": trigger,
                "func": self.__platform_subscribe,
                "func_kwargs": {
                    "platform": platform,
                    "page_cnt": page_cnt,
                },
            })
        return services

    def __build_service(self, sid: str, name: str, cron: str,
                        stype: str, page_cnt: int, popular_cnt: int) -> Optional[Dict[str, Any]]:
        """
        构造单个媒体类型的定时任务定义，cron 解析失败会向系统消息推送告警。
        """
        try:
            trigger = CronTrigger.from_crontab(cron)
        except Exception as err:
            logger.error(f"{name}定时任务配置错误：{err}")
            self.systemmessage.put(f"{name}执行周期配置错误：{err}")
            return None
        return {
            "id": sid,
            "name": name,
            "trigger": trigger,
            "func": self.__popular_subscribe,
            "func_kwargs": {
                "stype": stype,
                "page_cnt": page_cnt,
                "popular_cnt": popular_cnt,
            },
        }

    def __update_config(self):
        """
        将当前配置写回持久化存储。
        """
        self.update_config({
            "movie_enabled": self._movie_enabled,
            "tv_enabled": self._tv_enabled,
            "anime_enabled": self._anime_enabled,
            "movie_cron": self._movie_cron,
            "tv_cron": self._tv_cron,
            "anime_cron": self._anime_cron,
            "movie_page_cnt": self._movie_page_cnt,
            "tv_page_cnt": self._tv_page_cnt,
            "anime_page_cnt": self._anime_page_cnt,
            "movie_popular_cnt": self._movie_popular_cnt,
            "tv_popular_cnt": self._tv_popular_cnt,
            "anime_popular_cnt": self._anime_popular_cnt,
            "clear": self._clear,
            "clear_already_handle": self._clear_already_handle,
            "onlyonce": self._onlyonce,
            "username": self._username,
            "iqiyi_enabled": self._iqiyi_enabled,
            "tencent_enabled": self._tencent_enabled,
            "mangguo_enabled": self._mangguo_enabled,
            "iqiyi_cron": self._iqiyi_cron,
            "tencent_cron": self._tencent_cron,
            "mangguo_cron": self._mangguo_cron,
            "iqiyi_page_cnt": self._iqiyi_page_cnt,
            "tencent_page_cnt": self._tencent_page_cnt,
            "mangguo_page_cnt": self._mangguo_page_cnt,
            "netflix_enabled": self._netflix_enabled,
            "korean_enabled": self._korean_enabled,
            "netflix_cron": self._netflix_cron,
            "korean_cron": self._korean_cron,
            "netflix_page_cnt": self._netflix_page_cnt,
            "korean_page_cnt": self._korean_page_cnt,
            "western_enabled": self._western_enabled,
            "western_cron": self._western_cron,
            "western_page_cnt": self._western_page_cnt,
            "platform_recent_only": self._platform_recent_only,
            "platform_require_tmdb": self._platform_require_tmdb,
        })

    @staticmethod
    def __to_int(value: Any, default: int) -> int:
        """
        将配置项安全转成整数，失败回退到默认值。
        """
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @classmethod
    def __get_subscribe_statistic(cls, stype: str, count: int) -> List[dict]:
        """从 MoviePilot Server 读取热门订阅统计，并将响应收敛为字典列表。"""
        if not settings.SUBSCRIBE_STATISTIC_SHARE:
            return []

        server_host = (settings.MP_SERVER_HOST or "").strip().rstrip("/")
        if not server_host:
            logger.warning("MoviePilot Server 地址为空，无法读取热门订阅数据")
            return []

        try:
            response = RequestUtils(
                proxies=settings.PROXY,
                timeout=15,
                ua=settings.USER_AGENT,
            ).get_res(
                f"{server_host}{cls._SUBSCRIBE_STATISTIC_PATH}",
                params={"stype": stype, "page": 1, "count": max(int(count), 1)},
            )
        except Exception as err:
            logger.warning(f"读取{stype}热门订阅数据失败：{err}")
            return []

        if response is None or response.status_code != 200:
            logger.warning(f"读取{stype}热门订阅数据失败：服务端响应异常")
            return []

        try:
            payload = response.json()
        except (TypeError, ValueError):
            logger.warning(f"读取{stype}热门订阅数据失败：响应不是有效 JSON")
            return []

        # 外部服务当前返回裸列表，同时接受普通 envelope 以兼容服务端响应升级。
        if isinstance(payload, dict):
            if payload.get("success") is False:
                return []
            payload = payload.get("data")
        if not isinstance(payload, list):
            logger.warning(f"读取{stype}热门订阅数据失败：响应数据不是列表")
            return []
        return [item for item in payload if isinstance(item, dict)]

    @staticmethod
    def __parse_genre_ids(value: Any) -> set[int]:
        """将服务端逗号分隔或数组形式的分类 ID 归一为整数集合。"""
        if isinstance(value, str):
            values = value.split(",")
        elif isinstance(value, (list, tuple, set)):
            values = value
        elif isinstance(value, int) and not isinstance(value, bool):
            values = [value]
        else:
            values = []

        genre_ids = set()
        for item in values:
            try:
                genre_ids.add(int(str(item).strip()))
            except (TypeError, ValueError):
                continue
        return genre_ids

    @staticmethod
    def __history_unique(
            title: str,
            media_source: MediaSource,
            media_id: str,
            timestamp: str,
    ) -> str:
        """使用统一媒体身份构造订阅历史的稳定删除键。"""
        return f"popularsubscribe:{title}:{build_media_key(media_source, media_id)}:{timestamp}"

    @staticmethod
    def __detail_link(
            media_source: Optional[MediaSource],
            media_id: Optional[str],
            media_type: Optional[str],
    ) -> str:
        """按媒体来源生成历史详情页链接。"""
        media_source, media_id = resolve_media_identity(
            media_source=media_source,
            media_id=media_id,
        )
        if not media_source or not media_id:
            return ""
        if media_source == MediaSource.TMDB:
            path = "movie" if media_type in {PopularSubscribe._TYPE_MOVIE, "movie"} else "tv"
            return f"https://www.themoviedb.org/{path}/{media_id}"
        if media_source == MediaSource.Douban:
            return f"https://movie.douban.com/subject/{media_id}"
        if media_source == MediaSource.Bangumi:
            return f"https://bgm.tv/subject/{media_id}"
        if media_source == MediaSource.AniList:
            return f"https://anilist.co/anime/{media_id}"
        if media_source == MediaSource.IMDb:
            return f"https://www.imdb.com/title/{media_id}"
        if media_source == MediaSource.TVDB:
            return f"https://thetvdb.com/search?query={media_id}"
        return ""

    def __migrate_history_identity(self) -> None:
        """将存量历史中的来源专有 ID 幂等迁移为统一媒体身份。"""
        history = self.get_data("history")
        if not isinstance(history, list):
            return

        legacy_fields = (
            (MediaSource.TMDB, "tmdbid"),
            (MediaSource.Douban, "doubanid"),
            (MediaSource.Bangumi, "bangumiid"),
            (MediaSource.AniList, "anilistid"),
            (MediaSource.IMDb, "imdbid"),
            (MediaSource.TVDB, "tvdbid"),
        )
        legacy_keys = {key for _, key in legacy_fields}
        changed = False
        for item in history:
            if not isinstance(item, dict):
                continue

            media_source, media_id = resolve_media_identity(
                media_source=item.get("media_source"),
                media_id=item.get("media_id"),
            )
            if not media_source:
                for legacy_source, legacy_key in legacy_fields:
                    media_source, media_id = resolve_media_identity(
                        media_source=legacy_source,
                        media_id=item.get(legacy_key),
                    )
                    if media_source:
                        break
            if not media_source or not media_id:
                continue

            migrated = {
                key: value
                for key, value in item.items()
                if key not in legacy_keys
            }
            migrated["media_source"] = media_source.value
            migrated["media_id"] = media_id
            migrated["detail_link"] = migrated.get("detail_link") or self.__detail_link(
                media_source=media_source,
                media_id=media_id,
                media_type=migrated.get("type"),
            )
            migrated["unique"] = self.__history_unique(
                title=migrated.get("title") or "",
                media_source=media_source,
                media_id=media_id,
                timestamp=migrated.get("time") or "",
            )
            if migrated != item:
                item.clear()
                item.update(migrated)
                changed = True

        if changed:
            self.save_data("history", history)

    def __save_progress(self, history: List[dict], already_handle: List[str]) -> None:
        """持久化已完成候选，避免后续候选失败时丢失本轮进度。"""
        self.save_data("history", history)
        self.save_data("already_handle", already_handle)

    def __popular_subscribe(self, stype: str, page_cnt: int, popular_cnt: int):
        """
        拉取热门订阅统计并添加到本地订阅：
        - 电影/电视剧直接按 stype 查询；
        - 动漫复用电视剧数据源，按 TMDB 分类过滤，最多消费到目标条数为止。
        整体过程加锁，避免多个媒体类型并发写入历史记录时相互覆盖。
        """
        with self._history_lock:
            self.__do_popular_subscribe(stype=stype,
                                        page_cnt=page_cnt,
                                        popular_cnt=popular_cnt)

    def __run_all_enabled(self):
        """
        立即运行一次场景下的入口：按顺序执行三种媒体类型，避免并发。
        """
        if self._movie_enabled:
            logger.info("电影热门订阅：立即运行一次")
            self.__popular_subscribe(self._TYPE_MOVIE, self._movie_page_cnt, self._movie_popular_cnt)
        if self._tv_enabled:
            logger.info("电视剧热门订阅：立即运行一次")
            self.__popular_subscribe(self._TYPE_TV, self._tv_page_cnt, self._tv_popular_cnt)
        if self._anime_enabled:
            logger.info("动漫热门订阅：立即运行一次")
            self.__popular_subscribe(self._TYPE_ANIME, self._anime_page_cnt, self._anime_popular_cnt)
        for platform, enabled, page_cnt in (
            ("iqiyi", self._iqiyi_enabled, self._iqiyi_page_cnt),
            ("tencent", self._tencent_enabled, self._tencent_page_cnt),
            ("mangguo", self._mangguo_enabled, self._mangguo_page_cnt),
            ("netflix", self._netflix_enabled, self._netflix_page_cnt),
            ("korean", self._korean_enabled, self._korean_page_cnt),
            ("western", self._western_enabled, self._western_page_cnt),
        ):
            if enabled:
                logger.info(f"{self._PLATFORMS[platform]['name']}最新电视剧订阅：立即运行一次")
                self.__platform_subscribe(platform=platform, page_cnt=page_cnt)

    def __platform_subscribe(self, platform: str, page_cnt: int):
        """
        拉取视频平台最新电视剧并添加到本地订阅。
        整体过程加锁，避免与其它媒体类型并发写入历史记录时相互覆盖。
        """
        with self._history_lock:
            self.__do_platform_subscribe(platform=platform, page_cnt=page_cnt)

    def __fetch_platform_media(self, platform: str, page_cnt: int) -> List[Any]:
        """
        复用平台探索插件拉取最新电视剧列表。

        爱奇艺接口有风控，首次调用会自动通过无头浏览器获取 Cookie（约 20-30 秒）；
        腾讯视频按 sort=17（最新）拉取；芒果TV默认排序即最新。
        """
        page_cnt = max(self.__to_int(page_cnt, 10), 1)
        platform_name = self._PLATFORMS.get(platform, {}).get("name", platform)
        # Netflix / 韩剧：直接走 TMDB discover，不依赖额外的探索插件
        if platform in self._TMDB_SOURCES:
            return self.__fetch_tmdb_media(platform=platform, page_cnt=page_cnt)
        try:
            if platform == "iqiyi":
                # 延迟导入：平台探索插件可能未安装，避免影响插件自身加载
                from app.plugins.iqiyidiscover import IqiyiDiscover
                logger.info(f"{platform_name}：开始拉取最新电视剧（首次需自动获取Cookie，可能较慢）")
                response = asyncio.run(
                    IqiyiDiscover().iqiyi_discover(
                        apikey=settings.API_TOKEN,
                        mtype="tv",
                        mode="4",
                        page=1,
                        count=page_cnt,
                    )
                )
            elif platform == "tencent":
                from app.plugins.tencentvideodiscover import TencentVideoDiscover
                response = TencentVideoDiscover().tencentvideo_discover(
                    mtype="tv",
                    sort="17",
                    page=1,
                    count=page_cnt,
                )
            elif platform == "mangguo":
                from app.plugins.mangguodiscover import MangGuoDiscover
                response = MangGuoDiscover().mangguo_discover(
                    mtype="电视剧",
                    page=1,
                    count=page_cnt,
                )
            else:
                logger.warning(f"未知的平台数据源：{platform}")
                return []
        except ImportError as err:
            logger.error(
                f"拉取{platform_name}最新电视剧失败：未安装对应的平台探索插件（{err}），"
                f"请先在插件市场安装并启用该平台的 Discover 插件"
            )
            return []
        except Exception as err:
            logger.error(f"拉取{platform_name}最新电视剧失败：{err}")
            return []

        if response is None or not getattr(response, "success", False):
            logger.warning(f"拉取{platform_name}最新电视剧失败：响应异常")
            return []
        data = getattr(response, "data", None) or []
        # 统一收敛为带标题的媒体对象
        return [media for media in data if media and getattr(media, "title", None)]

    def __fetch_tmdb_media(self, platform: str, page_cnt: int) -> List[Any]:
        """
        通过 TMDB discover 拉取 Netflix 新剧 / 韩剧。

        - Netflix：with_networks=213（Netflix 自制网络）
        - 韩剧：with_origin_country=KR
        - 欧美剧：with_origin_country=US|GB|CA|AU|NZ|IE，并排除动画/少儿
        三者都按首播日期倒序，并用「上个月1日」作为首播日期下限。
        返回的条目本身已是 TMDB 身份，无需再做标题识别。
        """
        platform_name = self._PLATFORMS.get(platform, {}).get("name", platform)
        try:
            from app.modules.themoviedb.tmdbapi import TmdbApi
        except ImportError as err:
            logger.error(f"拉取{platform_name}新剧失败：TMDB 模块不可用（{err}）")
            return []

        params: Dict[str, Any] = {
            # 按热度排序（而不是首播日期），避免订阅到无人问津的冷门条
            "sort_by": "popularity.desc",
            "page": 1,
            "include_adult": "false",
            # 排除真人秀 / 脱口秀 / 新闻，避免订阅到综艺
            "without_genres": "10764,10767,10763",
            # 热度门槛：评分人数不足的直接跳过（新剧刚上线时票数少，门槛不宜过高）
            "vote_count.gte": self._TMDB_MIN_VOTES,
            "vote_average.gte": self._TMDB_MIN_RATING,
        }
        if self._recent_since:
            params["first_air_date.gte"] = self._recent_since.strftime("%Y-%m-%d")
        # 只取已经开播的（排除尚未上线的未来剧集）
        try:
            params["first_air_date.lte"] = datetime.now().strftime("%Y-%m-%d")
        except Exception:
            pass
        if platform == "netflix":
            params["with_networks"] = self._TMDB_NETFLIX_NETWORK
        elif platform == "korean":
            params["with_origin_country"] = "KR"
        else:
            # 欧美剧：多个出品国任一命中；额外排除动画/少儿
            params["with_origin_country"] = self._TMDB_WESTERN_COUNTRIES
            params["without_genres"] = "16,10762,10764,10767,10763"

        logger.info(f"{platform_name}：通过 TMDB discover 拉取最新剧集 {params}")
        try:
            infos = TmdbApi().discover_tvs(params) or []
        except Exception as err:
            logger.error(f"拉取{platform_name}最新剧集失败：{err}")
            return []

        medias: List[Any] = []
        for info in infos:
            if len(medias) >= page_cnt:
                break
            tmdb_id = info.get("id")
            name = info.get("name") or info.get("original_name")
            if not tmdb_id or not name:
                continue
            air_date = (info.get("first_air_date") or "").strip()
            year = air_date[:4] if len(air_date) >= 4 else None
            item = MediaInfo(
                type="电视剧",
                title=name,
                year=year,
                media_source=MediaSource("themoviedb"),
                media_id=str(tmdb_id),
                tmdb_id=int(tmdb_id),
                first_air_date=air_date or None,
                poster_path=info.get("poster_path"),
                overview=info.get("overview"),
                vote_average=info.get("vote_average"),
                original_language=info.get("original_language"),
            )
            # SDK 版 MediaInfo 没有 title_year 字段，补一个属性用于日志展示
            try:
                item.title_year = f"{name} ({year})" if year else name
            except Exception:
                pass
            medias.append(item)
        logger.info(f"{platform_name}：拉取到 {len(medias)} 条最新剧集")
        return medias

    def __do_platform_subscribe(self, platform: str, page_cnt: int):
        """
        执行单个视频平台的最新电视剧订阅（调用方须持有 _history_lock）。
        """
        platform_name = self._PLATFORMS.get(platform, {}).get("name", platform)
        medias = self.__fetch_platform_media(platform=platform, page_cnt=page_cnt)
        if not medias:
            logger.warning(
                f"未获取到{platform_name}最新电视剧数据，请确认已安装对应平台探索插件且网络可达"
            )
            return

        history_data = self.get_data("history")
        history: List[dict] = history_data if isinstance(history_data, list) else []
        already_handle_data = self.get_data("already_handle")
        already_handle: List[str] = (
            already_handle_data if isinstance(already_handle_data, list) else []
        )

        added_cnt = 0
        for media in medias:
            media_source, media_id = resolve_media_identity(media=media)
            if not media_source or not media_id:
                logger.warning(f"{getattr(media, 'title', '')} 缺少有效媒体身份，跳过")
                continue
            # 平台身份键与旧版 title_year 双重去重
            platform_key = f"{platform}:{media_id}"
            title_year = getattr(media, "title_year", None) or getattr(media, "title", "")
            if platform_key in already_handle or title_year in already_handle:
                logger.info(f"{title_year} 已被处理，跳过")
                continue

            # 元数据
            meta = MetaInfo(media.title)
            meta.type = MediaType.TV

            # --- 过滤一：短剧/未收录（要求能识别到 TMDB 条目） ---
            sub_media = media
            # TMDB 数据源（Netflix / 韩剧）本身就带 TMDB 身份，无需再次识别
            already_tmdb = (
                getattr(getattr(media, "media_source", None), "value", None) == "themoviedb"
            )
            if self._platform_require_tmdb and not already_tmdb:
                try:
                    recognized = self.chain.recognize_media(meta=meta, mtype=MediaType.TV)
                except Exception as err:
                    logger.warning(f"{title_year} TMDB 识别异常：{err}")
                    recognized = None
                if not recognized or not getattr(recognized, "tmdb_id", None):
                    logger.info(f"{title_year} 未识别到 TMDB 条目（短剧或未收录），跳过")
                    if platform_key not in already_handle:
                        already_handle.append(platform_key)
                        self.__save_progress(history, already_handle)
                    continue
                # 识别成功，后续用 TMDB 身份订阅（首播日期/库存/订阅判断更准确）
                sub_media = recognized

            # --- 过滤二：新剧（首播不早于上个月1日） ---
            if self._platform_recent_only and self._recent_since:
                air_raw = getattr(sub_media, "first_air_date", None) or ""
                is_new = False
                if air_raw:
                    try:
                        air_date = datetime.strptime(str(air_raw)[:10], "%Y-%m-%d").date()
                        is_new = air_date >= self._recent_since
                    except ValueError:
                        is_new = False
                if not is_new:
                    # 无精确首播日期时退回年份判断（年份不小于上月所在年份）
                    m_year = getattr(sub_media, "year", None)
                    try:
                        is_new = bool(m_year) and int(m_year) >= self._recent_since.year
                    except (TypeError, ValueError):
                        is_new = False
                if not is_new:
                    logger.info(
                        f"{title_year} 首播早于 {self._recent_since}（老剧），跳过新剧过滤"
                    )
                    if platform_key not in already_handle:
                        already_handle.append(platform_key)
                        self.__save_progress(history, already_handle)
                    continue

            # 媒体库存在性检查
            try:
                exist_flag, _ = self.downloadchain.get_no_exists_info(meta=meta, mediainfo=sub_media)
            except Exception as err:
                logger.warning(f"{title_year} 库存判断失败：{err}")
                continue
            if exist_flag:
                logger.info(f"{title_year} 媒体库中已存在，跳过")
                if platform_key not in already_handle:
                    already_handle.append(platform_key)
                    self.__save_progress(history, already_handle)
                continue
            # 订阅存在性检查
            try:
                subscribed = self.subscribechain.exists(mediainfo=sub_media, meta=meta)
            except Exception as err:
                logger.warning(f"{title_year} 订阅判断失败：{err}")
                continue
            if subscribed:
                logger.info(f"{title_year} 订阅已存在，跳过")
                if platform_key not in already_handle:
                    already_handle.append(platform_key)
                    self.__save_progress(history, already_handle)
                continue

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            detail_link = self._PLATFORMS.get(platform, {}).get("detail_link", "").format(
                media_id=media_id
            )
            # 订阅身份：识别成功时用 TMDB 身份，避免订阅沿用平台原身份
            if sub_media is not media:
                try:
                    resolved = resolve_media_identity(media=sub_media)
                    if resolved and resolved[0] and resolved[1]:
                        media_source, media_id = resolved
                except Exception:
                    pass
            history_item = {
                "title": media.title,
                "type": MediaType.TV.value,
                "year": getattr(sub_media, "year", None),
                "season": None,
                "poster": getattr(sub_media, "poster_path", None),
                "overview": getattr(sub_media, "overview", None),
                "media_source": media_source.value,
                "media_id": media_id,
                "detail_link": detail_link,
                "time": now_str,
                "platform": platform_name,
                "unique": self.__history_unique(
                    title=media.title,
                    media_source=media_source,
                    media_id=media_id,
                    timestamp=now_str,
                ),
            }

            # 添加订阅（平台来源统一按电视剧订阅，不指定季，订阅整剧）
            try:
                sid, msg = self.subscribechain.add(
                    title=sub_media.title or media.title,
                    year=getattr(sub_media, "year", None),
                    mtype=MediaType.TV,
                    media_source=media_source,
                    media_id=media_id,
                    season=None,
                    exist_ok=True,
                    username=self._username,
                )
            except Exception as err:
                logger.error(f"{title_year} 添加订阅失败：{err}")
                continue

            if not sid:
                logger.warning(f"{title_year} 添加订阅失败：{msg}")
                continue
            logger.info(f"{title_year}（{platform_name}）已添加订阅（{msg or '成功'}）")
            if platform_key not in already_handle:
                already_handle.append(platform_key)
            history.append(history_item)
            self.__save_progress(history, already_handle)
            added_cnt += 1

        self.__save_progress(history, already_handle)
        logger.info(f"{platform_name}最新电视剧订阅任务执行完成，本轮新增订阅 {added_cnt} 部")

    def __do_popular_subscribe(self, stype: str, page_cnt: int, popular_cnt: int):
        """
        执行单个媒体类型的热门订阅（调用方须持有 _history_lock）。
        """
        query_type = self._TYPE_TV if stype == self._TYPE_ANIME else stype
        page_limit = max(self.__to_int(page_cnt, 30), 1)
        # 动漫复用电视剧数据源，需要向服务端多要一些以便过滤
        query_count = page_limit * 20 if stype == self._TYPE_ANIME else page_limit

        subscribes = self.__get_subscribe_statistic(
            stype=query_type,
            count=query_count,
        )
        if not subscribes:
            logger.warning(
                f"未获取到{stype}热门订阅数据，请确认已开启‘订阅数据共享’或服务端可访问"
            )
            return

        history_data = self.get_data("history")
        history: List[dict] = history_data if isinstance(history_data, list) else []
        already_handle_data = self.get_data("already_handle")
        already_handle: List[str] = (
            already_handle_data if isinstance(already_handle_data, list) else []
        )

        anime_genre_ids = self.__parse_genre_ids(settings.ANIME_GENREIDS)
        matched_cnt = 0

        for sub in subscribes:
            if not isinstance(sub, dict):
                continue
            # 订阅人次门槛
            sub_count = self.__to_int(sub.get("count"), 0)
            if popular_cnt and sub_count < int(popular_cnt):
                logger.info(
                    f"{sub.get('name')} 订阅人次：{sub_count} 小于设定：{popular_cnt}，跳过"
                )
                continue

            # 构造媒体信息
            media = self.__build_media(sub)
            if not media or not media.title:
                continue
            media_source, media_id = resolve_media_identity(media=media)
            media_key = build_media_key(media_source, media_id)
            if not media_source or not media_id or not media_key:
                logger.warning(f"{media.title_year} 缺少有效媒体身份，跳过")
                continue

            # 电视剧/动漫分类过滤
            if stype in (self._TYPE_TV, self._TYPE_ANIME):
                if not self.__match_tv_or_anime(media=media,
                                                target_type=stype,
                                                anime_genre_ids=anime_genre_ids):
                    continue
                matched_cnt += 1
                if matched_cnt > page_limit:
                    break

            # 已处理跳过
            if media_key in already_handle or media.title_year in already_handle:
                logger.info(f"{media.type.value} {media.title_year} 已被处理，跳过")
                continue

            # 标题（含季度）
            season_str = None
            title = media.title_year
            if media.season and int(media.season) > 1:
                season_str = f"第{cn2an.an2cn(media.season, 'low')}季"
                title = f"{media.title_year} {season_str}"
            logger.info(f"{title} 订阅人次：{sub_count} 达到门槛：{popular_cnt or 0}")

            # 元数据
            meta = MetaInfo(media.title)
            meta.type = media.type
            if media.season:
                meta.begin_season = media.season
                meta.type = MediaType.TV

            # 媒体库/订阅存在性检查
            try:
                exist_flag, _ = self.downloadchain.get_no_exists_info(meta=meta, mediainfo=media)
            except Exception as err:
                logger.warning(f"{media.title_year} 库存判断失败：{err}")
                continue
            if exist_flag:
                logger.info(f"{media.title_year} 媒体库中已存在，跳过")
                if media_key not in already_handle:
                    already_handle.append(media_key)
                    self.__save_progress(history, already_handle)
                continue
            try:
                subscribed = self.subscribechain.exists(mediainfo=media, meta=meta)
            except Exception as err:
                logger.warning(f"{media.title_year} 订阅判断失败：{err}")
                continue
            if subscribed:
                logger.info(f"{media.title_year} 订阅已存在，跳过")
                if media_key not in already_handle:
                    already_handle.append(media_key)
                    self.__save_progress(history, already_handle)
                continue

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history_item = {
                "title": media.title,
                "type": media.type.value if media.type else stype,
                "year": media.year,
                "season": season_str,
                "poster": media.get_poster_image(),
                "overview": media.overview,
                "media_source": media_source.value,
                "media_id": media_id,
                "detail_link": media.detail_link,
                "time": now_str,
                "unique": self.__history_unique(
                    title=media.title,
                    media_source=media_source,
                    media_id=media_id,
                    timestamp=now_str,
                ),
            }

            # 添加订阅
            try:
                sid, msg = self.subscribechain.add(
                    title=media.title,
                    year=media.year,
                    mtype=media.type,
                    media_source=media_source,
                    media_id=media_id,
                    season=media.season if media.type == MediaType.TV else None,
                    exist_ok=True,
                    username=self._username,
                )
            except Exception as err:
                logger.error(f"{media.title_year} 添加订阅失败：{err}")
                continue

            if not sid:
                logger.warning(f"{media.title_year} 添加订阅失败：{msg}")
                continue
            logger.info(f"{media.title_year} 订阅人次：{sub_count} 已添加订阅（{msg or '成功'}）")
            if media_key not in already_handle:
                already_handle.append(media_key)
            history.append(history_item)
            self.__save_progress(history, already_handle)

        self.__save_progress(history, already_handle)
        logger.info(f"{stype}热门订阅任务执行完成")

    @staticmethod
    def __build_media(sub: dict) -> Optional[MediaInfo]:
        """
        根据服务端返回的订阅统计条目构造 MediaInfo。
        """
        media_source, media_id = resolve_media_identity(
            media_source=sub.get("media_source"),
            media_id=sub.get("media_id"),
        )
        if not media_source or not media_id:
            logger.warning(f"跳过缺少统一媒体身份的热门订阅：{sub.get('name')}")
            return None

        media = MediaInfo(media_source=media_source, media_id=media_id)
        media.title = sub.get("name")
        media.year = sub.get("year")
        media.season = PopularSubscribe.__to_int(sub.get("season"), 0) or None
        media.poster_path = sub.get("poster")
        media.backdrop_path = sub.get("backdrop")
        media.overview = sub.get("description")
        media.vote_average = sub.get("vote")
        media.genre_ids = list(PopularSubscribe.__parse_genre_ids(sub.get("genre_ids")))

        raw_type = sub.get("type")
        if isinstance(raw_type, MediaType):
            media.type = raw_type
        else:
            media.type = {
                "movie": MediaType.MOVIE,
                "电影": MediaType.MOVIE,
                "tv": MediaType.TV,
                "电视剧": MediaType.TV,
            }.get(str(raw_type or "").strip().casefold())
        if not media.type:
            logger.warning(f"跳过无法识别类型的热门订阅：{sub.get('name')}")
            return None
        return media

    def __get_genre_ids(self, media: MediaInfo) -> set[int]:
        """获取媒体分类；统计条目缺少分类时按统一身份补充 TMDB 信息。"""
        genre_ids = self.__parse_genre_ids(media.genre_ids)
        if genre_ids or not self.mediachain:
            return genre_ids

        tmdb_info = None
        if media.media_source == MediaSource.TMDB:
            try:
                tmdb_id = int(str(media.media_id))
            except (TypeError, ValueError):
                tmdb_id = 0
            if tmdb_id:
                try:
                    tmdb_info = self.mediachain.tmdb_info(
                        tmdbid=tmdb_id,
                        mtype=media.type,
                        season=media.season,
                    )
                except Exception as err:
                    logger.warning(f"{media.title_year} 获取 TMDB 分类失败：{err}")
        if isinstance(tmdb_info, dict):
            genre_ids = self.__parse_genre_ids(tmdb_info.get("genre_ids"))
            if genre_ids:
                media.genre_ids = list(genre_ids)
                return genre_ids

        try:
            supplemented = self.mediachain.supplement_tmdb_info(media)
        except Exception as err:
            logger.warning(f"{media.title_year} 补充 TMDB 分类失败：{err}")
            return set()
        genre_ids = self.__parse_genre_ids(getattr(supplemented, "genre_ids", None))
        if genre_ids:
            media.genre_ids = list(genre_ids)
        return genre_ids

    def __match_tv_or_anime(self, media: MediaInfo, target_type: str,
                            anime_genre_ids: set) -> bool:
        """
        通过统计条目携带的分类判断当前条目是否属于电视剧或动漫。
        无法识别分类时仅保留普通电视剧，避免把未知条目误订为动漫。
        """
        genre_ids = self.__get_genre_ids(media)
        if not genre_ids:
            return target_type == self._TYPE_TV

        is_anime = bool(genre_ids.intersection(anime_genre_ids))
        if target_type == self._TYPE_ANIME and not is_anime:
            logger.debug(f"{media.title_year} 不在动漫分类中，跳过")
            return False
        if target_type == self._TYPE_TV and is_anime:
            logger.debug(f"{media.title_year} 属于动漫分类，从电视剧订阅中跳过")
            return False
        return True

    def delete_history(self, key: str) -> schemas.Response[None]:
        """
        删除指定的历史记录条目。
        """
        historys = self.get_data("history")
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")
        historys = [h for h in historys if h.get("unique") != key]
        self.save_data("history", historys)
        return schemas.Response(success=True, message="删除成功")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        构造插件配置页面（Vuetify）与默认值。
        """
        return [
            {
                "component": "VForm",
                "content": [
                    self.__form_row_for_type(
                        enabled_key="movie_enabled", enabled_label="电影热门订阅",
                        cron_key="movie_cron", cron_label="电影订阅周期",
                        page_key="movie_page_cnt", page_label="电影获取条数",
                        popular_key="movie_popular_cnt", popular_label="电影订阅人次",
                    ),
                    self.__form_row_for_type(
                        enabled_key="tv_enabled", enabled_label="电视剧热门订阅",
                        cron_key="tv_cron", cron_label="电视剧订阅周期",
                        page_key="tv_page_cnt", page_label="电视剧获取条数",
                        popular_key="tv_popular_cnt", popular_label="电视剧订阅人次",
                    ),
                    self.__form_row_for_type(
                        enabled_key="anime_enabled", enabled_label="动漫热门订阅",
                        cron_key="anime_cron", cron_label="动漫订阅周期",
                        page_key="anime_page_cnt", page_label="动漫获取条数",
                        popular_key="anime_popular_cnt", popular_label="动漫订阅人次",
                    ),
                    self.__form_row_for_platform(
                        enabled_key="iqiyi_enabled", enabled_label="爱奇艺最新电视剧",
                        cron_key="iqiyi_cron", cron_label="爱奇艺订阅周期",
                        page_key="iqiyi_page_cnt", page_label="爱奇艺获取条数",
                    ),
                    self.__form_row_for_platform(
                        enabled_key="tencent_enabled", enabled_label="腾讯视频最新电视剧",
                        cron_key="tencent_cron", cron_label="腾讯订阅周期",
                        page_key="tencent_page_cnt", page_label="腾讯获取条数",
                    ),
                    self.__form_row_for_platform(
                        enabled_key="mangguo_enabled", enabled_label="芒果TV最新电视剧",
                        cron_key="mangguo_cron", cron_label="芒果订阅周期",
                        page_key="mangguo_page_cnt", page_label="芒果获取条数",
                    ),
                    self.__form_row_for_platform(
                        enabled_key="netflix_enabled", enabled_label="Netflix新剧（TMDB）",
                        cron_key="netflix_cron", cron_label="Netflix订阅周期",
                        page_key="netflix_page_cnt", page_label="Netflix获取条数",
                    ),
                    self.__form_row_for_platform(
                        enabled_key="korean_enabled", enabled_label="韩剧新剧（TMDB）",
                        cron_key="korean_cron", cron_label="韩剧订阅周期",
                        page_key="korean_page_cnt", page_label="韩剧获取条数",
                    ),
                    self.__form_row_for_platform(
                        enabled_key="western_enabled", enabled_label="欧美剧新剧（TMDB）",
                        cron_key="western_cron", cron_label="欧美剧订阅周期",
                        page_key="western_page_cnt", page_label="欧美剧获取条数",
                    ),
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "platform_recent_only",
                                            "label": "只订新剧（上个月以后首播）",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "platform_require_tmdb",
                                            "label": "过滤短剧（需识别到TMDB）",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "平台订阅：按周期拉取爱奇艺/腾讯视频/芒果TV的最新电视剧并自动添加订阅。"
                                                    "默认只订上个月以后首播的新剧，且过滤无法识别到TMDB的短剧（可在下方开关调整）。"
                                                    "需安装并启用对应平台的 Discover 探索插件（爱奇艺首次拉取会自动获取Cookie，约20-30秒）。"
                                                    "平台来源的媒体统一按电视剧订阅整剧，不指定季。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "数据来源于 MoviePilot 服务端订阅统计接口，需在‘设置-数据共享’中开启‘订阅数据共享’。"
                                                    "获取指定条数的热门媒体，达到订阅人次门槛后自动添加订阅。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": "立即运行一次：会对已开启的电影/电视剧/动漫订阅立刻执行一次；周期任务仍以 cron 表达式为准。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clear",
                                            "label": "清理订阅记录",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clear_already_handle",
                                            "label": "清理已处理记录",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "username",
                                            "label": "订阅用户",
                                            "placeholder": "默认为`热门订阅`",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "movie_enabled": False,
            "tv_enabled": False,
            "anime_enabled": False,
            "movie_cron": "5 1 * * *",
            "tv_cron": "5 1 * * *",
            "anime_cron": "5 1 * * *",
            "movie_page_cnt": 30,
            "tv_page_cnt": 30,
            "anime_page_cnt": 30,
            "movie_popular_cnt": 0,
            "tv_popular_cnt": 0,
            "anime_popular_cnt": 0,
            "onlyonce": False,
            "clear": False,
            "clear_already_handle": False,
            "username": "热门订阅",
            "iqiyi_enabled": False,
            "tencent_enabled": False,
            "mangguo_enabled": False,
            "iqiyi_cron": "30 8 * * *",
            "tencent_cron": "30 8 * * *",
            "mangguo_cron": "30 8 * * *",
            "iqiyi_page_cnt": 10,
            "tencent_page_cnt": 10,
            "mangguo_page_cnt": 10,
            "netflix_enabled": False,
            "korean_enabled": False,
            "netflix_cron": "0 3 * * *",
            "korean_cron": "10 3 * * *",
            "netflix_page_cnt": 20,
            "korean_page_cnt": 20,
            "western_enabled": False,
            "western_cron": "20 3 * * *",
            "western_page_cnt": 20,
            "platform_recent_only": True,
            "platform_require_tmdb": True,
        }

    @staticmethod
    def __form_row_for_platform(enabled_key: str, enabled_label: str,
                                cron_key: str, cron_label: str,
                                page_key: str, page_label: str) -> dict:
        """
        为单个视频平台（爱奇艺/腾讯视频/芒果TV）生成一行 3 列的配置面板。
        """
        return {
            "component": "VRow",
            "content": [
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 4},
                    "content": [
                        {
                            "component": "VSwitch",
                            "props": {"model": enabled_key, "label": enabled_label},
                        }
                    ],
                },
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 4},
                    "content": [
                        {
                            "component": "VTextField",
                            "props": {
                                "model": cron_key,
                                "label": cron_label,
                                "placeholder": "5位cron表达式，如 30 8 * * *",
                            },
                        }
                    ],
                },
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 4},
                    "content": [
                        {
                            "component": "VTextField",
                            "props": {
                                "model": page_key,
                                "label": page_label,
                                "placeholder": "10",
                            },
                        }
                    ],
                },
            ],
        }

    @staticmethod
    def __form_row_for_type(enabled_key: str, enabled_label: str,
                            cron_key: str, cron_label: str,
                            page_key: str, page_label: str,
                            popular_key: str, popular_label: str) -> dict:
        """
        为单个媒体类型（电影/电视剧/动漫）生成一行 4 列的配置面板。
        """
        return {
            "component": "VRow",
            "content": [
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 3},
                    "content": [
                        {
                            "component": "VSwitch",
                            "props": {"model": enabled_key, "label": enabled_label},
                        }
                    ],
                },
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 3},
                    "content": [
                        {
                            "component": "VTextField",
                            "props": {
                                "model": cron_key,
                                "label": cron_label,
                                "placeholder": "5位cron表达式，如 5 1 * * *",
                            },
                        }
                    ],
                },
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 3},
                    "content": [
                        {
                            "component": "VTextField",
                            "props": {
                                "model": page_key,
                                "label": page_label,
                                "placeholder": "30",
                            },
                        }
                    ],
                },
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 3},
                    "content": [
                        {
                            "component": "VTextField",
                            "props": {
                                "model": popular_key,
                                "label": popular_label,
                                "placeholder": "0 表示不限制",
                            },
                        }
                    ],
                },
            ],
        }

    def get_page(self) -> List[dict]:
        """
        订阅历史详情页，按时间倒序展示卡片，支持删除单条历史。
        """
        historys = self.get_data("history")
        if not historys:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {"class": "text-center"},
                }
            ]

        historys = sorted(historys, key=lambda x: x.get("time") or "", reverse=True)
        contents = [self.__build_history_card(item) for item in historys]
        return [
            {
                "component": "div",
                "props": {"class": "grid gap-3 grid-info-card"},
                "content": contents,
            }
        ]

    def __build_history_card(self, history: dict) -> dict:
        """
        为一条历史记录生成卡片元素。
        """
        title = history.get("title")
        year = history.get("year")
        season = history.get("season")
        poster = history.get("poster")
        mtype = history.get("type")
        time_str = history.get("time")
        media_source, media_id = resolve_media_identity(
            media_source=history.get("media_source"),
            media_id=history.get("media_id"),
        )
        detail_link = history.get("detail_link") or self.__detail_link(
            media_source=media_source,
            media_id=media_id,
            media_type=mtype,
        )
        unique = history.get("unique") or (
            self.__history_unique(
                title=title or "",
                media_source=media_source,
                media_id=media_id,
                timestamp=time_str or "",
            )
            if media_source and media_id
            else ""
        )

        info_texts = [f"类型：{mtype}", f"年份：{year}"]
        if season:
            info_texts.append(f"季度：{season}")
        platform_name = history.get("platform")
        if platform_name:
            info_texts.append(f"来源：{platform_name}")
        info_texts.append(f"时间：{time_str}")

        text_blocks = [
            {
                "component": "VCardText",
                "props": {"class": "pa-0 px-2"},
                "text": text,
            }
            for text in info_texts
        ]

        return {
            "component": "VCard",
            "content": [
                {
                    "component": "VDialogCloseBtn",
                    "props": {"innerClass": "absolute top-0 right-0"},
                    "events": {
                        "click": {
                            "api": "plugin/PopularSubscribe/delete_history",
                            "method": "get",
                            "params": {
                                "key": unique,
                            },
                        }
                    },
                },
                {
                    "component": "div",
                    "props": {"class": "d-flex justify-space-start flex-nowrap flex-row"},
                    "content": [
                        {
                            "component": "div",
                            "content": [
                                {
                                    "component": "VImg",
                                    "props": {
                                        "src": poster,
                                        "height": 120,
                                        "width": 80,
                                        "aspect-ratio": "2/3",
                                        "class": "object-cover shadow ring-gray-500",
                                        "cover": True,
                                    },
                                }
                            ],
                        },
                        {
                            "component": "div",
                            "content": [
                                {
                                    "component": "VCardSubtitle",
                                    "props": {
                                        "class": "pa-2 font-bold break-words whitespace-break-spaces"
                                    },
                                    "content": [
                                        {
                                            "component": "a",
                                            "props": {
                                                "href": detail_link,
                                                "target": "_blank",
                                            },
                                            "text": title,
                                        }
                                    ],
                                },
                                *text_blocks,
                            ],
                        },
                    ],
                },
            ],
        }

    def stop_service(self):
        """
        停止一次性调度器，周期任务由 MoviePilot 框架统一管理无需处理。
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as err:
            logger.error(f"退出热门订阅插件失败：{err}")
