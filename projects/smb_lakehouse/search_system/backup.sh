#!/bin/bash

# MySQL credentials
USER=root
PASSWORD=Inf0p0werc@rp

# Backup directory
BACKUP_DIR='/var/lib/backupFile'

# Log file
LOG_FILE='/var/lib/backupFile/mysqlbackup.log'

# Date and time
DATE=$(date +'%Y-%m-%d_%H-%M-%S')

# Backup file name
BACKUP_FILE="$BACKUP_DIR/all-databases-$DATE.sql"

# Create backup directory if it doesn't exist
mkdir -p $BACKUP_DIR

# Create backup
echo "Creating backup..." >>$LOG_FILE
docker exec -i mysql-business-only mysqldump -u $USER -p$PASSWORD --all-databases >$BACKUP_FILE 2>>$LOG_FILE

# Check if backup was successful
if [ $? -ne 0 ]; then
    echo "Backup failed at $(date)" >>$LOG_FILE
    exit 1
fi

# Compress backup
echo "Compressing backup..." >>$LOG_FILE
gzip $BACKUP_FILE

# Remove the uncompressed backup file
rm -f $BACKUP_FILE

# Check backup file, and keep only the latest 5 backups
echo "Cleaning up old backups..." >>$LOG_FILE
find $BACKUP_DIR -type f -name 'all-databases-*.sql.gz' -mtime +5 -exec rm {} \; >>$LOG_FILE 2>&1

# Transfer backup to B machine
# echo "Transferring backup to B machine..." >>$LOG_FILE
# rsync -avz --progress $BACKUP_FILE.gz infopower01@192.168.1.67:/opt/mysql >>$LOG_FILE 2>&1

# Record completion
echo "Backup completed at $(date)" >>$LOG_FILE