package ru.alishev.springcourse.shifttoscript;

import ru.tinkoff.piapi.contract.v1.*;
import ru.tinkoff.piapi.core.InvestApi;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AdvancedRobotDetection {
    // Параметры детекции
    private static final int MIN_SERIES_LENGTH = 3; // Мин. длина серии
    private static final double INTERVAL_TOLERANCE = 0.3; // Допуск вариации интервала (20%)
    private static final long TIME_WINDOW_MS = 240000; // Окно анализа
    private static final long MIN_LOT = 5; // Минимальный размер лота для учёта сделки
    private static final long LOT_TOLERANCE = 0; // Допустимое отклонение лота для "одинаковых" принтов
    private static final long AGGREGATION_WINDOW_MS = 50; // Окно агрегации тиков для снижения шума
    private static final long ROBOT_TIMEOUT_MS = 180000; // Время "молчания" робота, после которого он считается отключившимся

    // Буферы сделок по FIGI: Deque<(timestamp_ms, quantity)>
    private static final Map<String, Deque<long[]>> tradesBuffers = new HashMap<>();
    private static final Map<String, String> figiToTicker = new HashMap<>();

    // Активные роботы: ключ — FIGI + лот
    private static final Map<String, RobotState> activeRobots = new HashMap<>();

    // Формат и часовой пояс для вывода времени обнаружения (МСК)
    private static final ZoneId MOSCOW_ZONE = ZoneId.of("Europe/Moscow");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @SuppressWarnings("null")
    public static void main(String[] args) {
        String token = "t.J-x64pqvCTOfVs7QDlId7QLp7KZSZm4W-U7oPiC10eU_jQDuVv98LoYMQs2q98ArPwPoeWNVkyU0I54VDaLS6A"; // Ваш токен
        InvestApi api = InvestApi.create(token);

        ru.tinkoff.piapi.core.stream.MarketDataStreamService streamService = api.getMarketDataStreamService();
        var instrumentsService = api.getInstrumentsService();

        // Загружаем список российских акций и готовим FIGI для подписки
        List<String> figis = instrumentsService.getSharesSync(InstrumentStatus.INSTRUMENT_STATUS_BASE).stream()
                .filter(share -> "RUB".equalsIgnoreCase(share.getCurrency())) // российский рынок
                .peek(share -> figiToTicker.put(share.getFigi(), share.getTicker()))
                .map(Share::getFigi)
                .toList();

        // Response observer для получения данных
        StreamObserver<MarketDataResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(MarketDataResponse response) {
                if (response.hasTrade()) {
                    Trade trade = response.getTrade();
                    long timestampMs = trade.getTime().getSeconds() * 1000 + trade.getTime().getNanos() / 1_000_000;
                    long quantity = trade.getQuantity();
                    String figi = trade.getFigi();

                    // Фильтр по минимальному лоту
                    if (quantity < MIN_LOT) {
                        return;
                    }

                    // Добавляем в буфер по конкретному FIGI с агрегацией тиков в окно AGGREGATION_WINDOW_MS
                    Deque<long[]> buffer = tradesBuffers.computeIfAbsent(figi, k -> new ArrayDeque<>());
                    long[] last = buffer.peekLast();
                    if (last != null && (timestampMs - last[0]) <= AGGREGATION_WINDOW_MS) {
                        // Агрегируем: накапливаем объём и сдвигаем время на последний тик
                        last[0] = timestampMs;
                        last[1] += quantity;
                    } else {
                        buffer.addLast(new long[]{timestampMs, quantity});
                    }

                    // Удаляем старые (> TIME_WINDOW_MS)
                    long currentTime = Instant.now().toEpochMilli();
                    while (!buffer.isEmpty() && buffer.getFirst()[0] < currentTime - TIME_WINDOW_MS) {
                        buffer.removeFirst();
                    }

                    // Детекция, если достаточно данных
                    if (buffer.size() >= MIN_SERIES_LENGTH) {
                        detectRobots(figi, buffer);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Ошибка: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("Стрим завершён");
            }
        };

        // Создаём стрим через сервис SDK
        var subscription = streamService.newStream(
                "robot-detector",
                responseObserver::onNext,
                t -> {
                    System.err.println("Ошибка стрима: " + t.getMessage());
                    responseObserver.onError(t);
                }
        );

        // Подписка на сделки по FIGI
        subscription.subscribeTrades(figis);

        // Держим программу запущенной
        try {
            TimeUnit.DAYS.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void detectRobots(String figi, Deque<long[]> buffer) {
        List<Long> timestamps = new ArrayList<>(buffer.size());
        List<Long> sizes = new ArrayList<>(buffer.size());

        for (long[] trade : buffer) {
            timestamps.add(trade[0]);
            sizes.add(trade[1]);
        }

        // Анализируем только свежий хвост, чтобы шум старых сделок не "размазывал" интервалы
        int n = sizes.size();
        int start = Math.max(0, n - 10); // до 10 последних сделок
        List<Long> tsWindow = timestamps.subList(start, n);
        List<Long> sizesWindow = sizes.subList(start, n);

        // Текущее "логическое" время для этого FIGI — по последнему тику в окне
        long latestTs = tsWindow.get(tsWindow.size() - 1);

        String ticker = figiToTicker.getOrDefault(figi, figi);

        // Сначала очищаем список активных роботов по таймауту ROBOT_TIMEOUT_MS
        Iterator<Map.Entry<String, RobotState>> it = activeRobots.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, RobotState> entry = it.next();
            RobotState state = entry.getValue();
            if (!state.figi.equals(figi)) {
                continue; // чистим только роботов по текущему FIGI здесь
            }
            if (latestTs - state.lastTickMs > ROBOT_TIMEOUT_MS) {
                it.remove();
            }
        }

        // Группируем по "почти одинаковым" лотам с допуском LOT_TOLERANCE и ищем стабильные интервалы
        Map<Long, List<Long>> lotBuckets = new HashMap<>();
        outer:
        for (int i = 0; i < sizesWindow.size(); i++) {
            long size = sizesWindow.get(i);
            long ts = tsWindow.get(i);
            for (Long key : lotBuckets.keySet()) {
                if (Math.abs(key - size) <= LOT_TOLERANCE) {
                    lotBuckets.get(key).add(ts);
                    continue outer;
                }
            }
            lotBuckets.put(size, new ArrayList<>(List.of(ts)));
        }

        for (Map.Entry<Long, List<Long>> e : lotBuckets.entrySet()) {
            List<Long> tsList = e.getValue();
            if (tsList.size() < MIN_SERIES_LENGTH) continue;
            Double meanInterval = calculateMeanInterval(tsList);
            if (meanInterval == null) continue;

            long intervalSec = Math.round(meanInterval / 1000.0);
            long lastTickMs = tsList.get(tsList.size() - 1);

            long lot = e.getKey();
            String robotKey = figi + "|" + lot;

            RobotState state = activeRobots.get(robotKey);
            if (state == null) {
                // Новый робот — добавляем в список и логируем один раз
                state = new RobotState(figi, ticker, lot, intervalSec, lastTickMs);
                activeRobots.put(robotKey, state);

                String lastTickTimeMsk = Instant.ofEpochMilli(lastTickMs)
                        .atZone(MOSCOW_ZONE)
                        .toLocalTime()
                        .format(TIME_FORMATTER);

                System.out.println("Робот: " + ticker
                        + " Интервал=" + intervalSec + " секунд, лот≈" + lot
                        + " (±" + LOT_TOLERANCE + ")"
                        + " Время последнего тика (МСК)=" + lastTickTimeMsk);
            } else {
                // Уже известный робот — просто обновляем время последнего тика и интервал
                state.lastTickMs = lastTickMs;
                state.intervalSec = intervalSec;
            }
            return;
        }
    }

    // Состояние "робота" для конкретного FIGI и лота
    private static class RobotState {
        final String figi;
        final String ticker;
        final long lot;
        long intervalSec;
        final long firstDetectedMs;
        long lastTickMs;

        RobotState(String figi, String ticker, long lot, long intervalSec, long firstDetectedMs) {
            this.figi = figi;
            this.ticker = ticker;
            this.lot = lot;
            this.intervalSec = intervalSec;
            this.firstDetectedMs = firstDetectedMs;
            this.lastTickMs = firstDetectedMs;
        }
    }

    private static Double calculateMeanInterval(List<Long> timestamps) {
        if (timestamps.size() < 2) return null;

        double sumDiff = 0;
        for (int i = 1; i < timestamps.size(); i++) {
            sumDiff += timestamps.get(i) - timestamps.get(i - 1);
        }
        double mean = sumDiff / (timestamps.size() - 1);

        double sumSqDiff = 0;
        for (int i = 1; i < timestamps.size(); i++) {
            double diff = (timestamps.get(i) - timestamps.get(i - 1)) - mean;
            sumSqDiff += diff * diff;
        }
        double std = Math.sqrt(sumSqDiff / (timestamps.size() - 1));

        if (mean == 0 || std / mean > INTERVAL_TOLERANCE) {
            return null;
        }
        return mean;
    }

    private static String detectIdenticalLots(List<Long> sizes) {
        long first = sizes.get(0);
        for (long size : sizes) {
            if (Math.abs(size - first) > LOT_TOLERANCE) return null;
        }
        return String.valueOf(first);
    }

    private static String detectRepeatingPattern(List<Long> sizes) {
        int n = sizes.size();
        for (int pLen = 2; pLen <= Math.min(6, n / 2); pLen++) { // короткие паттерны более показательные
            List<Long> pattern = sizes.subList(0, pLen);
            boolean match = true;
            for (int i = pLen; i < n; i++) {
                long expected = pattern.get(i % pLen);
                if (Math.abs(sizes.get(i) - expected) > LOT_TOLERANCE) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return pattern.toString();
            }
        }
        return null;
    }
}