#!/bin/bash
# Script để optimize system cho low-latency SPSC queue
# Cần chạy với sudo

set -e

echo "=========================================="
echo "  System Optimization for Low Latency"
echo "=========================================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "ERROR: This script must be run as root (use sudo)"
    exit 1
fi

PRODUCER_CPU=6
CONSUMER_CPU=7

echo ""
echo "[1/7] Setting CPU frequency governor to 'performance'..."
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    if [ -f "$cpu" ]; then
        echo performance > $cpu
    fi
done
echo "✓ CPU governor set to performance"

echo ""
echo "[2/7] Disabling CPU frequency boost (for stable latency)..."
if [ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
    echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo
    echo "✓ Intel Turbo Boost disabled"
elif [ -f /sys/devices/system/cpu/cpufreq/boost ]; then
    echo 0 > /sys/devices/system/cpu/cpufreq/boost
    echo "✓ CPU boost disabled"
else
    echo "⚠ Could not find turbo boost control"
fi

echo ""
echo "[3/7] Checking hyper-threading siblings..."
PROD_SIBLINGS=$(cat /sys/devices/system/cpu/cpu$PRODUCER_CPU/topology/thread_siblings_list)
CONS_SIBLINGS=$(cat /sys/devices/system/cpu/cpu$CONSUMER_CPU/topology/thread_siblings_list)
echo "  CPU $PRODUCER_CPU siblings: $PROD_SIBLINGS"
echo "  CPU $CONSUMER_CPU siblings: $CONS_SIBLINGS"

# Extract sibling CPUs (not the main ones)
DISABLE_CPUS=""
for cpu in $(echo $PROD_SIBLINGS | tr ',' ' '); do
    if [ "$cpu" != "$PRODUCER_CPU" ]; then
        DISABLE_CPUS="$DISABLE_CPUS $cpu"
    fi
done
for cpu in $(echo $CONS_SIBLINGS | tr ',' ' '); do
    if [ "$cpu" != "$CONSUMER_CPU" ]; then
        DISABLE_CPUS="$DISABLE_CPUS $cpu"
    fi
done

if [ -n "$DISABLE_CPUS" ]; then
    echo "  Disabling HT siblings: $DISABLE_CPUS"
    for cpu in $DISABLE_CPUS; do
        if [ -f /sys/devices/system/cpu/cpu$cpu/online ]; then
            echo 0 > /sys/devices/system/cpu/cpu$cpu/online 2>/dev/null || true
        fi
    done
    echo "✓ Hyper-threading siblings disabled"
else
    echo "⚠ No HT siblings found to disable"
fi

echo ""
echo "[4/7] Moving IRQs away from CPUs $PRODUCER_CPU and $CONSUMER_CPU..."
# Calculate affinity mask excluding our CPUs
# This is a simple approach - move to CPUs 0-5
IRQ_MASK="3f"  # Binary: 00111111 = CPUs 0-5

IRQ_COUNT=0
for irq in /proc/irq/*; do
    if [ -d "$irq" ] && [ -f "$irq/smp_affinity" ]; then
        irq_num=$(basename $irq)
        if [[ "$irq_num" =~ ^[0-9]+$ ]]; then
            echo "$IRQ_MASK" > $irq/smp_affinity 2>/dev/null || true
            IRQ_COUNT=$((IRQ_COUNT + 1))
        fi
    fi
done
echo "✓ Moved $IRQ_COUNT IRQs away from CPUs $PRODUCER_CPU,$CONSUMER_CPU"

echo ""
echo "[5/7] Disabling automatic NUMA balancing..."
if [ -f /proc/sys/kernel/numa_balancing ]; then
    echo 0 > /proc/sys/kernel/numa_balancing
    echo "✓ NUMA balancing disabled"
else
    echo "⚠ NUMA balancing not available"
fi

echo ""
echo "[6/7] Setting swappiness to 0 (minimize swapping)..."
echo 0 > /proc/sys/vm/swappiness
echo "✓ Swappiness set to 0"

echo ""
echo "[7/7] Enabling transparent huge pages..."
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo always > /sys/kernel/mm/transparent_hugepage/enabled
    echo "✓ Transparent huge pages enabled"
else
    echo "⚠ Transparent huge pages not available"
fi

echo ""
echo "=========================================="
echo "  Optimization Complete!"
echo "=========================================="
echo ""
echo "Current Settings:"
echo "  CPU Governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
echo "  CPU $PRODUCER_CPU frequency: $(cat /sys/devices/system/cpu/cpu$PRODUCER_CPU/cpufreq/scaling_cur_freq) kHz"
echo "  CPU $CONSUMER_CPU frequency: $(cat /sys/devices/system/cpu/cpu$CONSUMER_CPU/cpufreq/scaling_cur_freq) kHz"
echo ""
echo "Note: Some optimizations require reboot to take full effect:"
echo "  - CPU isolation (isolcpus kernel parameter)"
echo "  - Disable timer ticks (nohz_full kernel parameter)"
echo ""
echo "To make these persistent, add to /etc/default/grub:"
echo "  GRUB_CMDLINE_LINUX=\"isolcpus=$PRODUCER_CPU,$CONSUMER_CPU nohz_full=$PRODUCER_CPU,$CONSUMER_CPU rcu_nocbs=$PRODUCER_CPU,$CONSUMER_CPU\""
echo ""
echo "Then run: sudo update-grub && sudo reboot"
echo ""
