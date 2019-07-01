
# Kive Media Server

Kive  - медиа сервер с открытым исходным кодом, разработанный для стриминга трансляций Vk Live.

Что умеет:
* RTMP для входящего потока
* HLS для зрителей
* DVR записи
* Кодеки AAC/H.264 для потока


## Скачиваем и запускаем kive
```bash
go get github.com/VKCOM/kive
cd $GOPATH/src/github.com/VKCOM/kive
docker build -t kive-docker .
mkdir /tmp/kive_docker # directoty where data files will be stored
docker run -p 8080:8080 -p 1935:1935 -v /tmp/kive_docker:/tmp/kive kive-docker kive-docker
```

## Скачиваем тестовый файл
```bash
curl -L 'https://github.com/VKCOM/kive/blob/master/test_assets/vk_sync.mp4?raw=true' --output /tmp/test.mp4
```

И стримим при помощи ffmpeg
```bash
ffmpeg -re -i /tmp/test.mp4 -c copy -f flv  "rtmp://127.0.0.1/live/teststream"
```

## Смотрим стрим

в браузере при помощи простого встроенного плеера (hls.js)
```
http://127.0.0.1:8080/player/kiveabr/teststream
```

при помощи ffplay текущую трансляцию
```bash
ffplay http://127.0.0.1:8080/live/teststream/playlist.m3u
```

при помощи ffplay для записи (в примере - последний час)
```bash
ffplay http://127.0.0.1:8080/live/teststream/playlist_dvr_range-$(date +%s -d '1 hour ago')-3600.m3u8
```