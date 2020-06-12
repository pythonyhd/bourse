import datetime
import os

import fitz


def pdf_image(pdfPath, imgPath, rotation_angle=0, zoom_x=1, zoom_y=1, ):
    # 打开PDF文件
    pdf = fitz.open(pdfPath)
    # 逐页读取PDF
    for pg in range(0, pdf.pageCount):
        page = pdf[pg]
        # 设置缩放和旋转系数
        trans = fitz.Matrix(zoom_x, zoom_y).preRotate(rotation_angle)
        pm = page.getPixmap(matrix=trans, alpha=False)
        # 开始写图像
        img_path = imgPath + str(pg) + ".png"
        pm.writePNG(imgPath + str(pg) + ".png")
        yield img_path
    pdf.close()


def pyMuPDF_fitz(pdfPath, imagePath):
    # doc = fitz.open('123.pdf')
    #
    # for pg in range(doc.pageCount):
    #     page = doc[pg]
    #     zoom = int(100)
    #     rotate = int(0)
    #     trans = fitz.Matrix(zoom / 100.0, zoom / 100.0).preRotate(rotate)
    #
    #     # create raster image of page (non-transparent)
    #     pm = page.getPixmap(matrix=trans, alpha=False)
    #
    #     # write a PNG image of the page
    #     pm.writePNG('%s.png' % pg)
    startTime_pdf2img = datetime.datetime.now()  # 开始时间

    print("imagePath=" + imagePath)
    pdfDoc = fitz.open(pdfPath)
    for pg in range(pdfDoc.pageCount):
        page = pdfDoc[pg]
        rotate = int(0)
        # 每个尺寸的缩放系数为1.3，这将为我们生成分辨率提高2.6的图像。
        # 此处若是不做设置，默认图片大小为：792X612, dpi=96
        zoom_x = 1.33333333  # (1.33333333-->1056x816)   (2-->1584x1224)
        zoom_y = 1.33333333
        mat = fitz.Matrix(zoom_x, zoom_y).preRotate(rotate)
        pix = page.getPixmap(matrix=mat, alpha=False)

        if not os.path.exists(imagePath):  # 判断存放图片的文件夹是否存在
            os.makedirs(imagePath)  # 若图片文件夹不存在就创建

        pix.writePNG(imagePath + '/' + 'images_%s.png' % pg)  # 将图片写入指定的文件夹内

    endTime_pdf2img = datetime.datetime.now()  # 结束时间
    print('pdf2img时间=', (endTime_pdf2img - startTime_pdf2img).seconds)


if __name__ == '__main__':
    pdfPath = './123.pdf'
    imagePath = './image'
    pyMuPDF_fitz(pdfPath, imagePath)
