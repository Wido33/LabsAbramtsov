package com.abramtsov;

class TextPrintStrategy implements PrintStrategy {
    @Override
    public void print() {
        System.out.println("Печатаем текст...");
    }
}