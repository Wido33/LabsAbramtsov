package com.abramtsov;

class ImagePrintStrategy implements PrintStrategy {
    @Override
    public void print() {
        System.out.println("Печатаем изображение...");
    }
}
