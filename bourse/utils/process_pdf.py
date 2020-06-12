# -*- coding: utf-8 -*-
import os

import fitz


def process_pdf_image(file_path, image_path):
    """PDF转成图片"""
    pdf_document = fitz.open(file_path)
    for page_num in range(pdf_document.pageCount):
        pdf_page = pdf_document[page_num]
        rotate = int(0)
        zoom_x = 1.33333333  # (1.33333333-->1056x816)   (2-->1584x1224)
        zoom_y = 1.33333333
        mat = fitz.Matrix(zoom_x, zoom_y).preRotate(rotate)
        pix = pdf_page.getPixmap(matrix=mat, alpha=False)
        if not os.path.exists(image_path):  # 判断存放图片的文件夹是否存在
            os.makedirs(image_path)  # 若图片文件夹不存在就创建

        pix.writePNG(image_path + '/' + 'images_%s.png' % page_num)  # 将图片写入指定的文件夹内


if __name__ == '__main__':
    pdfPath = './123.pdf'
    imagePath = './image'
    process_pdf_image(pdfPath, imagePath)