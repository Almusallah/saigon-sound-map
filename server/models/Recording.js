const mongoose = require('mongoose');

// Canonical (post-merge) categories — what new uploads use.
const NEW_CATEGORIES = [
  'Food & Drink', 'Sidewalks', 'Street Vendors', 'Music', 'Home',
  'Work', 'Repairing', 'Conversations', 'Vehicles', 'Markets',
  'Shops', 'Play & Leisure', 'Ritual & Ceremony',
  'Announcements & Signals', 'Infrastructure & Utilities',
  'Nature', 'Waterways', 'Background',
];

// Legacy categories kept for backward-compat with pre-merge records.
// Client maps these to NEW_CATEGORIES via CAT_ALIASES at display time.
const LEGACY_CATEGORIES = [
  'Eateries', 'Eating', 'Sports', 'Playing', 'Places of worship',
  'Construction Sites', 'Animals', 'Rain', 'Water', 'Boat', 'Others',
];

const CATEGORIES = [...NEW_CATEGORIES, ...LEGACY_CATEGORIES];

const recordingSchema = new mongoose.Schema({
  id:          { type: String, required: true, unique: true, index: true },
  title:       { type: String, required: true, trim: true },
  description: { type: String, default: '', trim: true },
  category:    { type: String, enum: CATEGORIES, default: 'Background' },
  audioUrl:    { type: String, required: true },
  // Optional photo attached to the recording — normalised WebP in B2
  // under images/ (see utils/image.js). Empty string when none.
  imageUrl:    { type: String, default: '' },
  latitude:    { type: Number, required: true },
  longitude:   { type: Number, required: true },
  // GeoJSON Point for proximity queries via 2dsphere index.
  // Coordinates order is [longitude, latitude] per GeoJSON spec.
  location: {
    type:        { type: String, enum: ['Point'], default: 'Point' },
    coordinates: { type: [Number], default: undefined },
  },
  source:      { type: String, enum: ['upload', 'b2-sync'], default: 'upload' },
  fileSize:    { type: Number, default: 0 },
  // Audio length in seconds, measured server-side with ffprobe at upload
  // (or by the backfill-durations admin job). Lets the client print
  // durations without fetching file metadata from B2.
  duration:    { type: Number, default: 0 },
}, {
  timestamps: true,
});

// Auto-populate GeoJSON location from lat/lng on save.
recordingSchema.pre('validate', function (next) {
  if (typeof this.latitude === 'number' && typeof this.longitude === 'number') {
    this.location = { type: 'Point', coordinates: [this.longitude, this.latitude] };
  }
  next();
});

recordingSchema.index({ latitude: 1, longitude: 1 });
recordingSchema.index({ location: '2dsphere' });
recordingSchema.index({ title: 'text', description: 'text' });

module.exports = mongoose.model('Recording', recordingSchema);
module.exports.CATEGORIES = CATEGORIES;
module.exports.NEW_CATEGORIES = NEW_CATEGORIES;
module.exports.LEGACY_CATEGORIES = LEGACY_CATEGORIES;
