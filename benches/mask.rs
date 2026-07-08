use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// --- Original (in-place) ---
#[inline]
fn unmask_easy(payload: &mut [u8], mask: [u8; 4]) {
    payload.iter_mut().enumerate().for_each(|(i, v)| *v ^= mask[i & 3]);
}

#[inline]
fn mask_inplace(buf: &mut [u8], mask: [u8; 4]) {
    let mask_u32 = u32::from_ne_bytes(mask);
    let (prefix, words, suffix) = unsafe { buf.align_to_mut::<u32>() };
    unmask_easy(prefix, mask);
    let head = prefix.len() & 3;
    let mask_u32 = if head > 0 {
        if cfg!(target_endian = "big") {
            mask_u32.rotate_left(8 * head as u32)
        } else {
            mask_u32.rotate_right(8 * head as u32)
        }
    } else {
        mask_u32
    };
    for word in words.iter_mut() { *word ^= mask_u32; }
    unmask_easy(suffix, mask_u32.to_ne_bytes());
}

// --- Your version (to Vec<u8>) ---
#[inline]
fn unmask_easy_to(payload: &[u8], mask: [u8; 4], output: &mut Vec<u8>) {
    output.extend(payload.iter().enumerate().map(|(i, &v)| v ^ mask[i & 3]));
}

#[inline]
fn mask_to(buf: &[u8], mask: [u8; 4], output: &mut Vec<u8>) {
    let mask_u32 = u32::from_ne_bytes(mask);
    let (prefix, words, suffix) = unsafe { buf.align_to::<u32>() };
    unmask_easy_to(prefix, mask, output);
    let head = prefix.len() & 3;
    let mask_u32 = if head > 0 {
        if cfg!(target_endian = "big") {
            mask_u32.rotate_left(8 * head as u32)
        } else {
            mask_u32.rotate_right(8 * head as u32)
        }
    } else {
        mask_u32
    };
    for word in words.iter() {
        output.extend_from_slice(&(*word ^ mask_u32).to_ne_bytes());
    }
    unmask_easy_to(suffix, mask_u32.to_ne_bytes(), output);
}

// --- Benchmark ---
fn copy_then_mask(buf: &[u8], mask: [u8; 4]) -> Vec<u8> {
    let mut copy = buf.to_vec();
    mask_inplace(&mut copy, mask);
    copy
}

fn mask_to_vec(buf: &[u8], mask: [u8; 4]) -> Vec<u8> {
    let mut output = Vec::with_capacity(buf.len());
    mask_to(buf, mask, &mut output);
    output
}

fn bench(c: &mut Criterion) {
    let buf = vec![0u8; 1024 * 1024]; // 1MB buffer
    let mask = [0x01, 0x02, 0x03, 0x04];

    let mut group = c.benchmark_group("mask_operations");
    group.throughput(Throughput::Bytes(buf.len() as u64));

    group.bench_function("copy_then_mask", |b| {
        b.iter(|| copy_then_mask(black_box(&buf), mask))
    });
    group.bench_function("mask_to_vec", |b| {
        b.iter(|| mask_to_vec(black_box(&buf), mask))
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);