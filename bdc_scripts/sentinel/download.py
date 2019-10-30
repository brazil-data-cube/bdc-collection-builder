import logging
import os
from bdc_scripts.utils import extractall, is_valid


def download(scene):
    cc = scene['sceneid'].split('_')
    yyyymm = cc[2][:4] + '-' + cc[2][4:6]
    # Output product dir
    productdir = '/S2_MSI/{}'.format(yyyymm)
    link = scene['link']
    sceneId = scene['sceneid']
    if not os.path.exists(productdir):
        os.makedirs(productdir)
    zfile = productdir + '/' + sceneId + '.zip'
    safeL1Cfull = productdir + '/' + sceneId + '.SAFE'

    logging.warning('downloadS2 - link {} file {}'.format(link, zfile))
    if not os.path.exists(safeL1Cfull):
        valid = True
        if os.path.exists(zfile):
            valid = is_valid(zfile)
        if not os.path.exists(zfile) or not valid:
            status = download_sentinel_images(link, zfile)
            if not status:
                return None

            # Check if file is valid
            valid = is_valid(zfile)

        if not valid:
            os.remove(zfile)
            return None
        else:
            extractall(zfile)

    return safeL1Cfull


def download_sentinel_images(link, zfile):
    get_s2_users()
    user = None
    for s2user in s2users:
        if s2users[s2user]['count'] < 2:
            user = s2user
            s2users[user]['count'] += 1
            break
    if user is None:
        logging.warning('doDownloadS2 - nouser')
        return False

    logging.warning('doDownloadS2 - user {} link {}'.format(user, link))

    try:
        response = requests.get(link, auth=(user, s2users[user]['password']), stream=True)
    except requests.exceptions.ConnectionError:
        logging.warning('doDownloadS2 - Connection Error')
        s2users[user]['count'] -= 1
        return False
    if 'Content-Length' not in response.headers:
        logging.warning(
            'doDownloadS2 - Content-Length not found for user {} in {} {}'.format(user, link, response.text))
        s2users[user]['count'] -= 1
        return False
    size = int(response.headers['Content-Length'].strip())
    if size < 30 * 1024 * 1024:
        logging.warning(
            'doDownloadS2 - user {} {} size {} MB too small'.format(user, zfile, int(size / 1024 / 1024)))
        s2users[user]['count'] -= 1
        return False
    logging.warning('doDownloadS2 - user {} {} size {} MB'.format(user, zfile, int(size / 1024 / 1024)))
    down = open(zfile, 'wb')

    chunk_size = 2048
    num_bars = int(size / chunk_size)
    file = os.path.basename(zfile)

    for chunk in tqdm(response.iter_content(chunk_size), total=num_bars, unit='KB', desc=file, leave=True):
        down.write(chunk)
    # for buf in response.iter_content(1024):
    #     down.write(buf)

    down.close()
    s2users[user]['count'] -= 1
    return True