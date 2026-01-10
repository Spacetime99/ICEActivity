cd /home/spacetime/codex/frontend
npm run build
sudo rsync -av --delete dist/ /var/www/ice-map/
sudo chown -R www-data:www-data /var/www/ice-map   # if needed

