//! I have just create this method for creating fake data
//! For a simple quick 'cargo test', i decided to use fake data.
// tests/test_utils.rs

use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};

use csv::WriterBuilder;
use serde::{Deserialize, Serialize};
use chrono::{NaiveDate, NaiveDateTime, Duration};
use rand::Rng;
use std::io::Write;

// --- Simulated Weather Data ---

#[derive(Debug, Serialize, Deserialize)]
pub struct SimulatedWeather {
    #[serde(rename = "sensor_id")]
    pub sensor_id: String,
    #[serde(rename = "location_label")]
    pub location_label: String,
    #[serde(rename = "region")]
    pub region: String,
    #[serde(rename = "recorded_day")]
    pub recorded_day: String,
    #[serde(rename = "avg_wind")]
    pub avg_wind: f32,
    #[serde(rename = "rainfall")]
    pub rainfall: f32,
    #[serde(rename = "snowfall")]
    pub snowfall: f32,
    #[serde(rename = "snow_depth")]
    pub snow_depth: f32,
    #[serde(rename = "max_temp")]
    pub max_temp: i32,
    #[serde(rename = "min_temp")]
    pub min_temp: i32,
}

pub fn create_weather_csv(dir: &Path, num_entries: usize) -> Result<PathBuf, Box<dyn Error>> {
    let file_path = dir.join("test_weather.csv");
    let file = File::create(&file_path)?;
    let mut wtr = WriterBuilder::new().from_writer(file);

    let start_date = NaiveDate::from_ymd_opt(2015, 1, 1).unwrap();
    let regions = ["East", "West", "North", "South"];
    let sensors = ["WX001", "WX002", "WX003", "WX004"];
    let labels = ["MetroHub A", "MetroHub B", "MetroHub C", "MetroHub D"];
    let mut rng = rand::thread_rng();

    for i in 0..num_entries {
        let region_idx = i % regions.len();
        let date = start_date + Duration::days(i as i64);

        let entry = SimulatedWeather {
            sensor_id: sensors[region_idx].to_string(),
            location_label: labels[region_idx].to_string(),
            region: regions[region_idx].to_string(),
            recorded_day: date.format("%Y-%m-%d").to_string(),
            avg_wind: rng.gen_range(0.0..20.0),
            rainfall: rng.gen_range(0.0..5.0),
            snowfall: rng.gen_range(0.0..10.0),
            snow_depth: rng.gen_range(0.0..10.0),
            max_temp: rng.gen_range(0..100),
            min_temp: rng.gen_range(-20..80),
        };
        wtr.serialize(entry)?;
    }

    wtr.flush()?;
    Ok(file_path)
}

// --- Simulated Ride Data ---

#[derive(Debug, Serialize, Deserialize)]
pub struct SimulatedRide {
    #[serde(rename = "provider")]
    pub provider: u8,
    #[serde(rename = "start_time")]
    pub start_time: String,
    #[serde(rename = "end_time")]
    pub end_time: String,
    #[serde(rename = "riders")]
    pub riders: u8,
    #[serde(rename = "distance_miles")]
    pub distance_miles: f32,
    #[serde(rename = "zone_type")]
    pub zone_type: u8,
    #[serde(rename = "flagged")]
    pub flagged: String,
    #[serde(rename = "start_zone")]
    pub start_zone: u16,
    #[serde(rename = "end_zone")]
    pub end_zone: u16,
    #[serde(rename = "payment_mode")]
    pub payment_mode: u8,
    #[serde(rename = "base_fare")]
    pub base_fare: f32,
    #[serde(rename = "surcharge")]
    pub surcharge: f32,
    #[serde(rename = "tax_fee")]
    pub tax_fee: f32,
    #[serde(rename = "gratuity")]
    pub gratuity: f32,
    #[serde(rename = "road_tolls")]
    pub road_tolls: f32,
    #[serde(rename = "service_fee")]
    pub service_fee: f32,
    #[serde(rename = "total_cost")]
    pub total_cost: f32,
    #[serde(rename = "traffic_charge")]
    pub traffic_charge: f32,
}

pub fn create_taxi_csv(dir: &Path, num_rides: usize) -> Result<PathBuf, Box<dyn Error>> {
    let file_path = dir.join("test_rides.csv");
    let file = File::create(&file_path)?;
    let mut wtr = WriterBuilder::new().from_writer(file);
    let mut rng = rand::thread_rng();

    let start_datetime = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2023, 6, 1).unwrap(),
        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );

    for _ in 0..num_rides {
        let pickup_offset_seconds = rng.gen_range(0..3600 * 24 * 15);
        let pickup_datetime = start_datetime + Duration::seconds(pickup_offset_seconds);
        let duration_seconds = rng.gen_range(300..2400);
        let dropoff_datetime = pickup_datetime + Duration::seconds(duration_seconds);

        let distance = rng.gen_range(0.5..15.0);
        let base_fare = distance * rng.gen_range(2.0..3.0);
        let gratuity = base_fare * rng.gen_range(0.1..0.25);
        let road_tolls = if rng.gen_bool(0.15) { rng.gen_range(0.0..12.0) } else { 0.0 };
        let traffic_charge = if rng.gen_bool(0.7) { 2.0 } else { 0.0 };

        let tax_fee = 0.5;
        let service_fee = 0.4;
        let surcharge = 2.5;

        let total_cost = base_fare + surcharge + tax_fee + gratuity + road_tolls + service_fee + traffic_charge;

        let ride = SimulatedRide {
            provider: rng.gen_range(1..=3),
            start_time: pickup_datetime.format("%Y-%m-%d %H:%M:%S").to_string(),
            end_time: dropoff_datetime.format("%Y-%m-%d %H:%M:%S").to_string(),
            riders: rng.gen_range(1..=5),
            distance_miles: distance,
            zone_type: rng.gen_range(1..=5),
            flagged: if rng.gen_bool(0.05) { "Y".to_string() } else { "N".to_string() },
            start_zone: rng.gen_range(100..300),
            end_zone: rng.gen_range(100..300),
            payment_mode: rng.gen_range(1..=4),
            base_fare,
            surcharge,
            tax_fee,
            gratuity,
            road_tolls,
            service_fee,
            total_cost,
            traffic_charge,
        };
        wtr.serialize(ride)?;
    }

    wtr.flush()?;
    Ok(file_path)
}


/// Generates a CSV with a single 'number' column (0..n-1).
pub fn create_simple_number_csv(base_dir: &Path, n: usize) -> std::io::Result<PathBuf> {
    let path = base_dir.join("numbers.csv");
    let mut file = File::create(&path)?;
    writeln!(file, "number")?;
    for i in 0..n {
        writeln!(file, "{}", i)?;
    }
    Ok(path)
}