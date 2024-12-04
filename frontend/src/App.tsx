import { useEffect, useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from "./components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table"
import { Badge } from "./components/ui/badge"
import { ScrollArea } from "./components/ui/scroll-area"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./components/ui/tabs"

interface StockData {
  stock_id: string
  current_price: number
  ema38: number
  ema100: number
  breakout?: string
  price_change?: number
  price_change_percent?: number
  trading_time: string
}

interface ApiResponse {
  timestamp: string
  stocks: StockData[]
}

function App() {
  const [stocksData, setStocksData] = useState<ApiResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:8002/api/stocks')
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`)
        }
        const data = await response.json()
        if (!data.stocks || !Array.isArray(data.stocks)) {
          throw new Error('Invalid data format received from server')
        }
        setStocksData(data)
        setError(null)
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Failed to fetch stock data'
        setError(`Error: ${errorMessage}. Make sure the backend server is running on port 8002.`)
        console.error(err)
      }
    }

    // Initial fetch
    fetchData()

    // Set up polling every 1 second
    const interval = setInterval(fetchData, 1000)

    // Cleanup
    return () => clearInterval(interval)
  }, [])

  const getBreakoutBadge = (breakout?: string) => {
    if (!breakout) return null
    
    const color = breakout === 'BULLISH_BREAKOUT' ? 'bg-green-500' : 'bg-red-500'
    return (
      <Badge className={`${color} text-white`}>
        {breakout.replace('_', ' ')}
      </Badge>
    )
  }

  const getPriceChangeColor = (change?: number) => {
    if (!change) return 'text-gray-500'
    return change > 0 ? 'text-green-500' : 'text-red-500'
  }

  return (
    <div className="min-h-screen bg-gray-100 p-8">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-3xl font-bold mb-8">Stock Market Dashboard</h1>
        
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          {/* Summary Cards */}
          <Card>
            <CardHeader>
              <CardTitle>Total Stocks</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{stocksData?.stocks.length || 0}</p>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader>
              <CardTitle>Active Breakouts</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">
                {stocksData?.stocks.filter(s => s.breakout).length || 0}
              </p>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader>
              <CardTitle>Last Update</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm">
                {stocksData?.timestamp ? new Date(stocksData.timestamp).toLocaleTimeString() : '-'}
              </p>
            </CardContent>
          </Card>
        </div>

        <Tabs defaultValue="all" className="w-full">
          <TabsList>
            <TabsTrigger value="all">All Stocks</TabsTrigger>
            <TabsTrigger value="breakouts">Breakouts</TabsTrigger>
          </TabsList>

          <TabsContent value="all">
            <Card>
              <CardHeader>
                <CardTitle>Stock Prices and EMAs</CardTitle>
              </CardHeader>
              <CardContent>
                <ScrollArea className="h-[600px] w-full">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Stock ID</TableHead>
                        <TableHead>Price</TableHead>
                        <TableHead>EMA38</TableHead>
                        <TableHead>EMA100</TableHead>
                        <TableHead>Change</TableHead>
                        <TableHead>Status</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {stocksData?.stocks.map((stock) => (
                        <TableRow key={stock.stock_id}>
                          <TableCell className="font-medium">{stock.stock_id}</TableCell>
                          <TableCell>${stock.current_price.toFixed(2)}</TableCell>
                          <TableCell>{stock.ema38.toFixed(2)}</TableCell>
                          <TableCell>{stock.ema100.toFixed(2)}</TableCell>
                          <TableCell className={getPriceChangeColor(stock.price_change)}>
                            {stock.price_change_percent ? 
                              `${stock.price_change_percent > 0 ? '+' : ''}${stock.price_change_percent.toFixed(2)}%` 
                              : '-'}
                          </TableCell>
                          <TableCell>{getBreakoutBadge(stock.breakout)}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="breakouts">
            <Card>
              <CardHeader>
                <CardTitle>Active Breakouts</CardTitle>
              </CardHeader>
              <CardContent>
                <ScrollArea className="h-[600px] w-full">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Stock ID</TableHead>
                        <TableHead>Pattern</TableHead>
                        <TableHead>Price</TableHead>
                        <TableHead>EMA38</TableHead>
                        <TableHead>EMA100</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {stocksData?.stocks
                        .filter(stock => stock.breakout)
                        .map((stock) => (
                          <TableRow key={stock.stock_id}>
                            <TableCell className="font-medium">{stock.stock_id}</TableCell>
                            <TableCell>{getBreakoutBadge(stock.breakout)}</TableCell>
                            <TableCell>${stock.current_price.toFixed(2)}</TableCell>
                            <TableCell>{stock.ema38.toFixed(2)}</TableCell>
                            <TableCell>{stock.ema100.toFixed(2)}</TableCell>
                          </TableRow>
                        ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        {error && (
          <Card className="mt-4 bg-red-50">
            <CardContent className="pt-6">
              <p className="text-red-500">{error}</p>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  )
}

export default App
