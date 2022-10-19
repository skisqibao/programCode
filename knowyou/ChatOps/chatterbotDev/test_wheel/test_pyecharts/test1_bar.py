from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.render import make_snapshot
import os

from snapshot_phantomjs import snapshot

file_path = "{}/".format(os.path.dirname(os.path.abspath(__file__)))

def bar_chart() -> Bar:
    c = (
        Bar(init_opts=opts.InitOpts(js_host=file_path))
        .add_xaxis(["衬衫", "毛衣", "领带", "裤子", "风衣", "高跟鞋", "袜子"])
        .add_yaxis("商家A", [114, 55, 27, 101, 125, 27, 105])
        .add_yaxis("商家B", [57, 134, 137, 129, 145, 60, 49])
        .reversal_axis()
        .set_series_opts(label_opts=opts.LabelOpts(position="right"))
        .set_global_opts(title_opts=opts.TitleOpts(title="Bar-测试渲染图片"))
    )
    return c

make_snapshot(snapshot, bar_chart().render(), "bar0.png")