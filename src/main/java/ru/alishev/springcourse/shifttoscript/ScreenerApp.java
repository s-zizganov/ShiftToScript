package ru.alishev.springcourse.shifttoscript;

import ru.tinkoff.piapi.core.InvestApi;

public class ScreenerApp {
    public static void main(String[] args) {
        String token = "t.J-x64pqvCTOfVs7QDlId7QLp7KZSZm4W-U7oPiC10eU_jQDuVv98LoYMQs2q98ArPwPoeWNVkyU0I54VDaLS6A"; // Замените на реальный токен
        InvestApi api = InvestApi.create(token); // Prod-контур
        // Для sandbox: InvestApi.createSandbox(token)
    }
}