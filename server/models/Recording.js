const mongoose = require('mongoose');

const CATEGORIES = [
  'Eateries', 'Sidewalks', 'Street Vendors', 'Music', 'Home',
  'Eating', 'Work', 'Repairing', 'Water', 'Animals',
  'Conversations', 'Vehicles', 'Sports', 'Places of worship',
  'Markets', 'Shops', 'Construction Sites', 'Playing',
  'Rain', 'Boat', 'Others'
];

const recordingSchema = new mongoose.Schema({
  id:          { type: String, required: true, unique: true, index: true },
  title:       { type: String, required: true, trim: true },
  description: { type: String, default: '', trim: true },
  category:    { type: String, enum: CATEGORIES, default: 'Others' },
  audioUrl:    { type: String, required: true },
  latitude:    { type: Number, required: true },
  longitude:   { type: Number, required: true },
  source:      { type: String, enum: ['upload', 'b2-sync'], default: 'upload' },
  fileSize:    { type: Number, default: 0 },
}, {
  timestamps: true,
});

recordingSchema.index({ latitude: 1, longitude: 1 });
recordingSchema.index({ title: 'text', description: 'text' });

module.exports = mongoose.model('Recording', recordingSchema);
module.exports.CATEGORIES = CATEGORIES;
