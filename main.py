import matplotlib.pyplot as plt
from google.cloud import bigquery

JOB_ID = "ジョブID"

bq = bigquery.Client(location="ロケーション")
stages = bq.get_job(JOB_ID).query_plan

# ジョブ情報を解析
labels = {}
data = []
dependency = []
for stage in stages:
    stage_id = int(stage.entry_id)
    data.append(
        (
            stage_id,
            int(stage.start.timestamp() * 1000),
            int(stage.end.timestamp() * 1000),
        )
    )
    labels[stage_id] = stage.name
    for i in stage.input_stages:
        dependency.append((i, stage_id))

# 開始を0に合わせる
total_start = min(d[1] for d in data)
data = [(job, start_ms - total_start, end_ms - total_start) for job, start_ms, end_ms in data]

data.sort(key=lambda t: t[1])

# チャートの初期設定
fig, ax = plt.subplots()
ax.set_xlabel("Time (ms)")
ax.set_title("Job Execution Timeline")
ax.grid(False)

# y軸のラベルを非表示にする
ax.set_yticks([])

# 各ジョブの位置を保存するための辞書
positions = {}

# 各ジョブを描画し、ラベルを配置
for i, (job_id, start_ms, end_ms) in enumerate(data):
    positions[job_id] = i  # ジョブのインデックスを保存
    print(job_id, end_ms - start_ms, start_ms)
    duration = end_ms - start_ms
    ax.barh(i, duration, left=start_ms, height=1)
    # ラベルをバーの中央あたりに配置
    job_name = labels[job_id]
    ax.text(
        start_ms + duration / 2,
        i,
        f"{job_name}: {duration} ms",
        ha="center",
        fontsize=5,
        va="center",
        color="black",
    )

# 依存関係に基づいて線を描画
for dep_from, dep_to in dependency:
    from_index = positions[dep_from]
    to_index = positions[dep_to]
    from_end_time = [end_ms for job, start_ms, end_ms in data if job == dep_from][0]
    to_start_time = [start_ms for job, start_ms, end_ms in data if job == dep_to][0]
    plt.plot(
        [from_end_time, to_start_time],
        [from_index, to_index],
        color="red",
        linestyle="-",
        linewidth=0.5,
    )

# y軸の反転を設定
ax.invert_yaxis()

# 描画を表示
plt.show()
