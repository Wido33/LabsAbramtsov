package com.abramtsov;

class Printer {
    private PrintStrategy strategy;

    public void setStrategy(PrintStrategy strategy) {
        this.strategy = strategy;
    }

    public void executePrint() {
        if (strategy != null) {
            strategy.print();
        } else {
            System.out.println("Не выбрана стратегия печати!");
        }
    }
}