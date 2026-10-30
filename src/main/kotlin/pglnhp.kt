import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.*

// --- Модель данных ---
data class SensorReading(
    val sensorId: String,
    val value: Double,
    val timestamp: Long
)

// --- Корутины: параллельная генерация событий через канал ---
suspend fun emitSensorData(channel: SendChannel<SensorReading>) = coroutineScope {
    // Внутри создаём корутину, которая генерирует 10 показаний
    launch {
        repeat(10) { i ->
            delay(500)
            val reading = SensorReading(
                sensorId = "TEMP", // будет переопределён в main через параметр
                value = 15.0 + i * 2.0,
                timestamp = System.currentTimeMillis()
            )
            channel.send(reading)
        }
    }
}

// Перегрузка с указанием ID датчика — так удобнее запускать 3 корутины
suspend fun emitSensorData(sensorId: String, channel: SendChannel<SensorReading>) = coroutineScope {
    launch {
        repeat(10) { i ->
            delay(500)
            val reading = SensorReading(
                sensorId = sensorId,
                value = when (sensorId) {
                    "TEMP" -> 15.0 + i * 2.0   // от 15 до 33
                    "HUM" -> 40.0 + i * 3.0    // от 40 до 67
                    "CO2" -> 18.0 + i * 1.5    // от 18 до 31.5
                    else -> 20.0 + i
                },
                timestamp = System.currentTimeMillis()
            )
            channel.send(reading)
        }
    }
}

// --- Flow: ленивый поток с операциями ---
fun sensorFlow(sensorId: String): Flow<SensorReading> = flow {
    repeat(10) { i ->
        delay(300)
        val value = when (sensorId) {
            "TEMP" -> 15.0 + i * 2.0
            "HUM" -> 40.0 + i * 3.0
            "CO2" -> 18.0 + i * 1.5
            else -> 20.0 + i
        }
        emit(SensorReading(sensorId, value, System.currentTimeMillis()))
    }
}

// --- MAIN ---
fun main() = runBlocking {
    println("=== 1) Корутины + Channel: параллельная генерация ===")

    val channel = Channel<SensorReading>()

    // Запускаем 3 корутины-датчика параллельно
    val job1 = launch { emitSensorData("TEMP", channel) }
    val job2 = launch { emitSensorData("HUM", channel) }
    val job3 = launch { emitSensorData("CO2", channel) }

    // Читаем все 30 показаний вручную в цикле for
    var count = 0
    for (msg in channel) {
        println("Получено: $msg")
        count++
        if (count == 30) break // 3 датчика × 10 показаний
    }

    // Ждём завершения всех корутин-производителей
    job1.join()
    job2.join()
    job3.join()
    channel.close()

    println("\n=== 2) Flow: ленивый поток с операциями ===")

    // ВАЖНО: до вызова терминальной операции (toList) поток молчит
    val flow = sensorFlow("TEMP")
        .filter { it.value > 20.0 }          // отбросить низкие значения
        .map { it.copy(value = it.value * 1.1) } // калибровка
        .take(5)                              // взять только первые 5
        .onEach { println("Обработано: $it") } // логирование

    println("Поток создан, но ещё не выполняется...")

    // Терминальная операция запускает поток
    val results = flow.toList()
    println("Результаты toList(): $results")

    println("\n=== 3) Агрегация через fold ===")

    // fold — терминальная операция, запускает поток заново
    val sum = sensorFlow("TEMP")
        .filter { it.value > 20.0 }
        .map { it.copy(value = it.value * 1.1) }
        .take(5)
        .fold(0.0) { acc, reading -> acc + reading.value }

    val average = sum / 5
    println("Сумма: $sum")
    println("Среднее: $average")

    println("\n=== Смена завершена ===")
}
