FROM php:7.3-cli-alpine3.10

# https://getcomposer.org/doc/03-cli.md#composer-allow-superuser
ENV COMPOSER_ALLOW_SUPERUSER 1
ARG HOST_USER_ID
ARG HOST_USER

# PHP: Copy configuration files & remove dist files
RUN mkdir /phpIni
COPY dev/php/files/bin/ /usr/local/bin/

# SYS: Install required packages
RUN apk --no-cache upgrade && \
    apk --no-cache add bash git sudo openssh autoconf gcc g++ make gettext

RUN apk add librdkafka librdkafka-dev \
        --update-cache --repository http://dl-3.alpinelinux.org/alpine/edge/community

#sadly apple users run into a problem since the AD user has a higher ID than 65k :(
#RUN adduser -u $HOST_USER_ID -D -H $HOST_USER
#so we do it oldschool
RUN echo "$HOST_USER:x:$HOST_USER_ID:82:Linux User,,,:/home/$HOST_USER:" >> /etc/passwd && \
    echo "$HOST_USER:!:$(($(date +%s) / 60 / 60 / 24)):0:99999:7:::" >> /etc/shadow && \
    echo "$HOST_USER:x:$HOST_USER_ID:" >> /etc/group && \
    mkdir /home/$HOST_USER && \
    chown $HOST_USER:$HOST_USER /home/$HOST_USER && \
    echo "ALL ALL=NOPASSWD: ALL" >> /etc/sudoers && \
    addgroup $HOST_USER www-data

# COMPOSER: install binary
RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/bin --filename=composer

# COMPOSER: install dependencies
RUN composer global require hirak/prestissimo

# PHP: Install php extensions
RUN pecl channel-update pecl.php.net && \
    pecl install rdkafka && \
    docker-php-ext-install pcntl && \
    php-ext-enable rdkafka pcntl

USER $HOST_USER

WORKDIR /var/www/html

