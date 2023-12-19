import java.util.LinkedList
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicIntegerArray
import kotlin.system.measureTimeMillis

class Graph(private val array: Array<IntArray>) {
    fun neighbours(i: Int) = array[i]

    val size: Int
        get() = array.size

    override fun toString(): String {
        return array.contentDeepToString()
    }
}

fun seqBFS(graph: Graph, start: Int): IntArray {
    val queue = LinkedList<Int>()
    val distances = IntArray(graph.size)
    val visited = HashSet<Int>()
    queue.add(start)
    distances[start] = 0
    visited.add(start)
    while (queue.isNotEmpty()) {
        val vert = queue.poll()
        for (i in graph.neighbours(vert)) {
            if (!visited.add(i)) continue
            queue.add(i)
            distances[i] = distances[vert] + 1
        }
    }
    return distances
}

const val PFOR_THRESHOLD = 1 shl 14

fun pfor(pool: ForkJoinPool, start: Int, end: Int, call: (Int) -> Unit) {
    if (end - start < PFOR_THRESHOLD) {
        for (i in start until end) call(i)
        return
    }
    val m = (start + end) / 2
    val f1 = pool.submit { pfor(pool, start, m, call) }
    val f2 = pool.submit { pfor(pool, m, end, call) }
    f1.join()
    f2.join()
}

const val PSCAN_THRESHOLD = 1 shl 14

fun pscan(pool: ForkJoinPool, data: IntArray): IntArray {
    val sums = IntArray(data.size * 4 / PSCAN_THRESHOLD + 2 * PSCAN_THRESHOLD)
    val res = IntArray(data.size + 1)

    fun countSums(l: Int, r: Int, idx: Int) {
        if (r - l < PSCAN_THRESHOLD) {
            var s = 0
            for (i in l until r) s += data[i]
            sums[idx] = s
            return
        }
        val m = (l + r) / 2
        val f1 = pool.submit { countSums(l, m, idx * 2) }
        val f2 = pool.submit { countSums(m, r, idx * 2 + 1) }
        f1.join()
        f2.join()
        sums[idx] = sums[idx * 2] + sums[idx * 2 + 1]
    }

    fun propSums(l: Int, r: Int, idx: Int, sum: Int) {
        if (r - l < PSCAN_THRESHOLD) {
            var s = sum
            for (i in l until r) {
                s += data[i]
                res[i + 1] = s
            }
            return
        }
        val m = (l + r) / 2
        val f1 = pool.submit { propSums(l, m, idx * 2, sum) }
        val f2 = pool.submit { propSums(m, r, idx * 2 + 1, sum + sums[idx * 2]) }
        f1.join()
        f2.join()
    }

    countSums(0, data.size, 1)
    propSums(0, data.size, 1, 0)
    return res
}

fun pBFS(graph: Graph, start: Int, pool: ForkJoinPool = ForkJoinPool.commonPool()): IntArray {
    val distances = IntArray(graph.size)
    val visited = AtomicIntegerArray(graph.size)
    visited.set(0, 1)
    distances[start] = 0

    var layer = intArrayOf(start)
    var d = 0
    while (layer.isNotEmpty()) {
        val degree = IntArray(layer.size).also {
            pfor(pool, 0, layer.size) { i -> it[i] = graph.neighbours(layer[i]).size }
        }.let { pscan(pool, it) }
        val nextLayer = IntArray(degree.last()) { -1 }
        d++
        pfor(pool, 0, layer.size) { i ->
            val neighbors = graph.neighbours(layer[i])
            for (j in neighbors.indices) {
                val next = neighbors[j]
                if (!visited.compareAndSet(next, 0, 1)) continue
                distances[next] = d
                nextLayer[degree[i] + j] = next
            }
        }
        layer = nextLayer.filter { it != -1 }.toIntArray()
    }
    return distances
}

fun makeCubeGraph(side: Int): Graph {
    fun idx(x: Int, y: Int, z: Int) = x + y * side + z * side * side

    return Array(side * side * side) { i ->
        val x = i % side
        val y = (i / side) % side
        val z = (i / side / side) % side
        val res = ArrayList<Int>()
        if (x > 0) res.add(idx(x - 1, y, z))
        if (x < side - 1) res.add(idx(x + 1, y, z))
        if (y > 0) res.add(idx(x, y - 1, z))
        if (y < side - 1) res.add(idx(x, y + 1, z))
        if (z > 0) res.add(idx(x, y, z - 1))
        if (z < side - 1) res.add(idx(x, y, z + 1))
        res.toIntArray()
    }.let { Graph(it) }
}

fun main() {
    fun testBFS() {
        val gs = ArrayList<Graph>()
        arrayOf(
            intArrayOf(1, 2),
            intArrayOf(0, 2),
            intArrayOf(0, 1, 3),
            intArrayOf(2, 4, 5, 6),
            intArrayOf(3, 5, 7),
            intArrayOf(3, 4),
            intArrayOf(3, 8),
            intArrayOf(4),
            intArrayOf(6, 9),
            intArrayOf(8, 10),
            intArrayOf(9, 11),
            intArrayOf(10, 12),
            intArrayOf(11, 13, 16),
            intArrayOf(12, 14),
            intArrayOf(13, 15),
            intArrayOf(14, 16),
            intArrayOf(12, 15),
        ).let { Graph(it) }.also { gs.add(it) }


        gs.forEach { g ->
            println(seqBFS(g, 0).contentToString())
            println(pBFS(g, 0).contentToString())
        }
    }

    fun testPfor() {
        val ar = IntArray(1000_000)
        pfor(ForkJoinPool.commonPool(), 0, 1000_000) { ar[it]++ }
        println(ar.sum())
    }

    fun testPScan() {
        pscan(ForkJoinPool.commonPool(), IntArray(100) { 1 }).also {
            println(it.contentToString())
        }
    }

    fun testCubeGraph() {
        println(makeCubeGraph(2))
    }

    fun testLarge(): Pair<Long, Long> {
        val g = makeCubeGraph(300)
//        val ar1 : IntArray
//        val ar2 : IntArray
        val t1 = measureTimeMillis {
            seqBFS(g, 0)
        }.also { println("Seq $it") }
        val t2 = measureTimeMillis {
            pBFS(g, 0, ForkJoinPool(4))
        }.also { println("Par $it") }
//        println(ar1)
//        println(ar2)
//        println(ar1.contentEquals(ar2))
        return t1 to t2
    }

//    testPScan()
//    testBFS()

//    testCubeGraph()

    (0..5).map { testLarge() }.drop(1).fold(0L to 0L) { p1, p2 ->
        p1.first + p2.first to p1.second + p2.second
    }.also { println("Seq avg: ${it.first / 5}, Par avg: ${it.second / 5}") }
        .also { println("Ratio is ${it.first.toDouble().div(it.second.toDouble())}") }

}