
import { useEffect, useState } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { Activity, Server, Database, HardDrive, Clock, Cpu } from 'lucide-react'
import { cn } from './lib/utils'

// Types for our metrics
interface Metrics {
  activeWorkers: number
  shardsLoaded: number
  bytesRead: number
  checkpointsWritten: number
  currentEpoch: number
  shardLoadLatency: number
}

interface TimeSeriesPoint {
  time: string
  throughput: number // MB/s
  latency: number // ms
}

function App() {
  const [metrics, setMetrics] = useState<Metrics>({
    activeWorkers: 0,
    shardsLoaded: 0,
    bytesRead: 0,
    checkpointsWritten: 0,
    currentEpoch: 0,
    shardLoadLatency: 0,
  })

  const [history, setHistory] = useState<TimeSeriesPoint[]>([])
  const [connected, setConnected] = useState(false)

  // Parse Prometheus format text
  const parseMetrics = (text: string): Metrics => {
    const lines = text.split('\n')
    const values: any = {}

    lines.forEach(line => {
      if (line.startsWith('#') || line.trim() === '') return
      const [key, val] = line.split(' ')
      if (key && val) values[key] = parseFloat(val)
    })

    // Approximate mapping based on our known metric names
    return {
      activeWorkers: values['vortex_active_workers'] || 0,
      shardsLoaded: values['vortex_shards_loaded_total'] || 0,
      bytesRead: values['vortex_bytes_read_total'] || 0,
      checkpointsWritten: values['vortex_checkpoints_written_total'] || 0,
      currentEpoch: values['vortex_current_epoch'] || 0,
      // Using sum/count to get average if available, else 0
      shardLoadLatency: (values['vortex_shard_load_duration_seconds_sum'] || 0) / (values['vortex_shard_load_duration_seconds_count'] || 1),
    }
  }

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const response = await fetch('http://localhost:9100/metrics')
        const text = await response.text()
        const current = parseMetrics(text)

        setMetrics(current)
        setConnected(true)

        // Calculate throughput (diff from previous bytes would be better, but for MVP total is fine or we can calc diff)
        // For MVP chart, let's just show total loaded or a mock rate if we don't store prev

        setHistory(prev => {
          const now = new Date().toLocaleTimeString()
          const newPoint = {
            time: now,
            throughput: current.bytesRead / 1024 / 1024, // Total MB read so far
            latency: current.shardLoadLatency * 1000 // ms
          }
          const newHistory = [...prev, newPoint]
          if (newHistory.length > 20) newHistory.shift()
          return newHistory
        })

      } catch (e) {
        console.error("Failed to fetch metrics", e)
        setConnected(false)
      }
    }

    const interval = setInterval(fetchMetrics, 1000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="min-h-screen bg-background text-foreground dark font-sans p-8">
      <header className="mb-8 flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
            Vortex Mission Control
          </h1>
          <p className="text-muted-foreground mt-1">Distributed Training Orchestrator Status</p>
        </div>
        <div className={cn("flex items-center gap-2 px-3 py-1 rounded-full text-sm font-medium border", connected ? "bg-green-500/10 text-green-500 border-green-500/20" : "bg-red-500/10 text-red-500 border-red-500/20")}>
          <div className={cn("w-2 h-2 rounded-full", connected ? "bg-green-500" : "bg-red-500")} />
          {connected ? "System Online" : "Disconnected"}
        </div>
      </header>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
        <Card title="Active Workers" value={metrics.activeWorkers} icon={Server} color="text-blue-500" />
        <Card title="Current Epoch" value={metrics.currentEpoch} icon={Activity} color="text-purple-500" />
        <Card title="Total Data Read" value={`${(metrics.bytesRead / 1024 / 1024).toFixed(2)} MB`} icon={Database} color="text-green-500" />
        <Card title="Shards Loaded" value={metrics.shardsLoaded} icon={HardDrive} color="text-orange-500" />
      </div>

      <div className="grid gap-4 md:grid-cols-7 mb-8">
        <div className="col-span-4 rounded-xl border bg-card p-6 shadow-sm">
          <h3 className="font-semibold mb-4 flex items-center gap-2">
            <Cpu className="w-4 h-4 text-primary" />
            Throughput History (Total MB)
          </h3>
          <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={history}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis dataKey="time" stroke="#888" fontSize={12} />
                <YAxis stroke="#888" fontSize={12} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f1f1f', border: '1px solid #333' }}
                  itemStyle={{ color: '#fff' }}
                />
                <Line type="monotone" dataKey="throughput" stroke="#3b82f6" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="col-span-3 rounded-xl border bg-card p-6 shadow-sm">
          <h3 className="font-semibold mb-4 flex items-center gap-2">
            <Clock className="w-4 h-4 text-primary" />
            Performance Stats
          </h3>
          <div className="space-y-4">
            <div className="flex justify-between items-center p-3 bg-secondary/50 rounded-lg">
              <span className="text-sm text-muted-foreground">Shard Load Latency</span>
              <span className="font-mono font-medium">{(metrics.shardLoadLatency * 1000).toFixed(1)} ms</span>
            </div>
            <div className="flex justify-between items-center p-3 bg-secondary/50 rounded-lg">
              <span className="text-sm text-muted-foreground">Checkpoints Written</span>
              <span className="font-mono font-medium">{metrics.checkpointsWritten}</span>
            </div>
            <div className="flex justify-between items-center p-3 bg-secondary/50 rounded-lg">
              <span className="text-sm text-muted-foreground">Est. Throughput</span>
              <span className="font-mono font-medium">
                {history.length > 1 ? (
                  (history[history.length - 1].throughput - history[0].throughput) / (history.length)
                ).toFixed(2) : "0.00"} MB/s
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function Card({ title, value, icon: Icon, color }: any) {
  return (
    <div className="rounded-xl border bg-card p-6 shadow-sm flex items-center justify-between space-x-4">
      <div>
        <p className="text-sm font-medium text-muted-foreground">{title}</p>
        <h2 className="text-2xl font-bold">{value}</h2>
      </div>
      <div className={cn("p-3 rounded-full bg-secondary/50", color)}>
        <Icon className="w-6 h-6" />
      </div>
    </div>
  )
}

export default App
