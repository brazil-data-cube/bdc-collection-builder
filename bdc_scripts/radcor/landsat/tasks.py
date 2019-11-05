# Python Native
import logging

# 3rdparty Libraries
from bdc_scripts.celery import celery_app


class LandsatTask(celery_app.Task):
    def download(self, scene):
        global SESSION
        app.logger.warning('downloadLC8 - scene {}'.format(scene))
        cc = scene['sceneid'].split('_')
        pathrow = cc[2]
        yyyymm = cc[3][:4]+'-'+cc[3][4:6]
    # Output product dir
        productdir = '/LC8/{}/{}'.format(yyyymm,pathrow)
        if not os.path.exists(productdir):
            os.makedirs(productdir)

        link = scene['link']
        app.logger.warning('downloadLC8 - link {}'.format(link))
        getSESSION()
        r = SESSION.get(link, stream=True)
        app.logger.warning('downloadLC8 - r {}'.format(r.headers))
        count = 0
        while r.headers.get("Content-Disposition") is None and count < 2:
            app.logger.warning('downloadLC8 - Content-Disposition not found for {}'.format(link))
            count += 1
            cc = link.split('/')
            sid = cc[-3]
            last = ord(sid[-1])+1
            last = chr(last)
            cc[-3] = sid[:-1]+last
            link = '/'.join(cc)
            r = SESSION.get(link, stream=True)
        if count == 2:
            return None
        outtar = os.path.join(productdir, r.headers.get("Content-Disposition").split('=')[1])
        app.logger.warning('downloadLC8 - outtar {}'.format(outtar))
        if r.headers.get("Content-length") is None:
            app.logger.warning('downloadLC8 - Content-Length not found for {}'.format(link))
            return None
        total_size = int(r.headers.get("Content-length"))
        app.logger.warning( 'downloadLC8 - {} to {} size {}'.format(link,outtar,int(total_size/1024/1024)))
        file_size = 0
        if os.path.exists(outtar):
            file_size = os.path.getsize(outtar)
            app.logger.warning( 'downloadLC8 - {} to {} link_size {} file_size {}'.format(link,outtar,total_size,file_size))
        if total_size == file_size:
            app.logger.warning( 'downloadLC8 - {} already downloaded'.format(link))
            return outtar

        block_size = 1024*10
        part = 0
        with open(outtar, 'wb') as fs:
            for chunk in r.iter_content(chunk_size=block_size):
                if chunk:
                    fs.write(chunk)
                    part += block_size
        return outtar

    def publish(self, scene):
        pass

    def upload(self, scene):
        pass


@celery_app.task(base=LandsatTask, queue='download')
def download_landsat(scene):
    return download_landsat.download(scene)


@celery_app.task(base=LandsatTask, queue='publish')
def publish_landsat(scene):
    return publish_landsat.publish(scene)


@celery_app.task(base=LandsatTask, queue='upload')
def upload_landsat(scene):
    upload_landsat.upload(scene)
