package com.abramtsov;

public class Main {
    public static void main(String[] args) {
        Printer printer = new Printer();

        // Использование текстовой стратегии
        printer.setStrategy(new TextPrintStrategy());
        printer.executePrint();

        // Использование стратегии печати изображений
        printer.setStrategy(new ImagePrintStrategy());
        printer.executePrint();

        // Использование стратегии печати графиков
        printer.setStrategy(new GraphPrintStrategy());
        printer.executePrint();
    }
}