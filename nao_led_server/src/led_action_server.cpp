// Copyright 2024 Antonio Bono
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "nao_led_server/led_action_server.hpp"

#include <chrono>
#include <functional>

#include "rclcpp_components/register_node_macro.hpp"

namespace nao_led_action_server
{

using namespace std::chrono_literals;

static inline bool is_valid_eff(uint8_t eff)
{
  return eff < nao_led_interfaces::msg::LedIndexes::NUMLEDS;
}

static inline bool has_led(const std::array<uint8_t, 2> & leds, uint8_t idx)
{
  return leds[0] == idx || leds[1] == idx;
}

static inline bool is_color_zero_rgb(const std_msgs::msg::ColorRGBA & c)
{
  return (c.r == 0.0F && c.g == 0.0F && c.b == 0.0F);
}

static inline void normalize_alpha(std_msgs::msg::ColorRGBA & c)
{
  if (c.a == 0.0F) {
    c.a = 1.0F;
  }
}

std::array<std_msgs::msg::ColorRGBA, 8>
LedsPlayActionServer::normalize_eye_colors(const std::array<std_msgs::msg::ColorRGBA, 8> & in)
{
  std::array<std_msgs::msg::ColorRGBA, 8> out = in;
  for (auto & c : out) {
    normalize_alpha(c);
  }

  bool any_nonzero_after0 = false;
  for (int i = 1; i < 8; ++i) {
    if (!is_color_zero_rgb(out[i])) {
      any_nonzero_after0 = true;
      break;
    }
  }

  if (!any_nonzero_after0 && !is_color_zero_rgb(out[0])) {
    for (int i = 1; i < 8; ++i) {
      out[i] = out[0];
      normalize_alpha(out[i]);
    }
  }

  return out;
}

LedsPlayActionServer::LedsPlayActionServer(const rclcpp::NodeOptions & options)
: rclcpp::Node("leds_play_action_server_node", options)
{
  using std::placeholders::_1;
  using std::placeholders::_2;

  action_server_ = rclcpp_action::create_server<LedsPlay>(
    this,
    "leds_play",
    std::bind(&LedsPlayActionServer::handleGoal, this, _1, _2),
    std::bind(&LedsPlayActionServer::handleCancel, this, _1),
    std::bind(&LedsPlayActionServer::handleAccepted, this, _1));

  head_pub_ = create_publisher<nao_lola_command_msgs::msg::HeadLeds>("effectors/head_leds", 10);
  right_eye_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightEyeLeds>("effectors/right_eye_leds", 10);
  left_eye_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftEyeLeds>("effectors/left_eye_leds", 10);
  right_ear_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightEarLeds>("effectors/right_ear_leds", 10);
  left_ear_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftEarLeds>("effectors/left_ear_leds", 10);
  chest_pub_ = create_publisher<nao_lola_command_msgs::msg::ChestLed>("effectors/chest_led", 10);
  right_foot_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightFootLed>("effectors/right_foot_led", 10);
  left_foot_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftFootLed>("effectors/left_foot_led", 10);

  color_off_.r = 0.0F;
  color_off_.g = 0.0F;
  color_off_.b = 0.0F;
  color_off_.a = 1.0F;

  for (auto & g : eff_gen_) {
    g.store(0, std::memory_order_relaxed);
  }

  RCLCPP_INFO(get_logger(), "LedsPlayActionServer Initialized");
}

LedsPlayActionServer::~LedsPlayActionServer()
{
  // Nothing special: each goal runs in a detached thread and will exit naturally.
  // We only need to clear token map (best-effort).
  std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
  goal_tokens_.clear();
}

rclcpp_action::GoalResponse LedsPlayActionServer::handleGoal(
  const rclcpp_action::GoalUUID & uuid,
  std::shared_ptr<const LedsPlay::Goal> goal)
{
  (void)uuid;
  (void)goal;
  RCLCPP_INFO(get_logger(), "Received LED goal request");
  return rclcpp_action::GoalResponse::ACCEPT_AND_EXECUTE;
}

rclcpp_action::CancelResponse LedsPlayActionServer::handleCancel(
  const std::shared_ptr<GoalHandleLedsPlay> goal_handle)
{
  (void)goal_handle;
  RCLCPP_INFO(get_logger(), "Received request to cancel LED goal");
  return rclcpp_action::CancelResponse::ACCEPT;
}

void LedsPlayActionServer::handleAccepted(const std::shared_ptr<GoalHandleLedsPlay> goal_handle)
{
  // Per-effector preemption: bump generation only for effectors referenced by this goal.
  const auto goal = goal_handle->get_goal();
  const auto leds = goal->leds;

  std::array<std::uint64_t, kNumEffectors> tokens{};
  tokens.fill(0);

  for (int i = 0; i < 2; ++i) {
    const uint8_t eff = leds[i];
    if (!is_valid_eff(eff)) {
      continue;
    }
    // increment generation => preempt previous configuration for this effector
    const auto new_gen = eff_gen_[eff].fetch_add(1, std::memory_order_acq_rel) + 1;
    tokens[eff] = new_gen;
  }

  {
    std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
    goal_tokens_[goal_handle.get()] = tokens;
  }

  // Keep the same "structure": detach an execution thread for this goal.
  std::thread([this, goal_handle]() { execute(goal_handle); }).detach();
}

bool LedsPlayActionServer::shouldStop(const std::shared_ptr<GoalHandleLedsPlay> & goal_handle) const
{
  if (!rclcpp::ok()) {
    return true;
  }
  if (goal_handle && goal_handle->is_canceling()) {
    return true;
  }
  return false;
}

void LedsPlayActionServer::execute(const std::shared_ptr<GoalHandleLedsPlay> goal_handle)
{
  RCLCPP_INFO(get_logger(), "Executing LED goal");

  const auto goal = goal_handle->get_goal();

  const std::array<uint8_t, 2> leds = goal->leds;
  const uint8_t mode = goal->mode;
  const float frequency = goal->frequency;
  const std::array<std_msgs::msg::ColorRGBA, 8> colors = goal->colors;
  const std::array<float, 12> intensities = goal->intensities;
  const float duration = goal->duration;

  // fetch tokens for this goal (captured in handleAccepted)
  std::array<std::uint64_t, kNumEffectors> tokens{};
  tokens.fill(0);
  {
    std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
    auto it = goal_tokens_.find(goal_handle.get());
    if (it != goal_tokens_.end()) {
      tokens = it->second;
    }
  }

  auto result = std::make_shared<LedsPlay::Result>();
  bool stopped_early = false;

  auto any_requested = [&]() -> bool {
    for (int i = 0; i < 2; ++i) {
      if (is_valid_eff(leds[i])) {
        return true;
      }
    }
    return false;
  };

  auto owned = [&](uint8_t eff) -> bool {
    if (!is_valid_eff(eff)) {
      return false;
    }
    if (!has_led(leds, eff)) {
      return false;
    }
    const auto tok = tokens[eff];
    if (tok == 0) {
      return false;
    }
    return eff_gen_[eff].load(std::memory_order_acquire) == tok;
  };

  if (!any_requested()) {
    result->success = false;
    if (goal_handle->is_active()) {
      try {
        goal_handle->abort(result);
      } catch (const rclcpp::exceptions::RCLError & e) {
        RCLCPP_WARN(get_logger(), "Failed to abort goal (RCLError): %s", e.what());
      }
    }
    // cleanup token map
    std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
    goal_tokens_.erase(goal_handle.get());
    return;
  }

  // Execute mode. Each mode now internally stops publishing for an effector if it's preempted.
  if (mode == LedModes::STEADY) {
    stopped_early = steadyMode(goal_handle, leds, colors, intensities, duration);
  } else if (mode == LedModes::BLINKING) {
    stopped_early = blinkingMode(goal_handle, leds, frequency, colors, duration);
  } else if (mode == LedModes::LOOP) {
    stopped_early = loopMode(goal_handle, leds, frequency, colors, duration);
  } else {
    publishOff(leds);
    result->success = false;
    if (goal_handle->is_active()) {
      try {
        goal_handle->abort(result);
      } catch (const rclcpp::exceptions::RCLError & e) {
        RCLCPP_WARN(get_logger(), "Failed to abort goal (RCLError): %s", e.what());
      }
    }
    std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
    goal_tokens_.erase(goal_handle.get());
    return;
  }

  // Goal finalization (ONLY from this thread).
  if (stopped_early) {
    if (goal_handle->is_canceling()) {
      result->success = true;
      try {
        goal_handle->canceled(result);
      } catch (const rclcpp::exceptions::RCLError & e) {
        RCLCPP_WARN(get_logger(), "Failed to mark goal canceled (RCLError): %s", e.what());
      }
    } else {
      // stopped early due to preemption of all targeted effectors, or ROS shutdown
      result->success = false;
      if (goal_handle->is_active()) {
        try {
          goal_handle->abort(result);
        } catch (const rclcpp::exceptions::RCLError & e) {
          RCLCPP_WARN(get_logger(), "Failed to abort goal (RCLError): %s", e.what());
        }
      }
    }

    std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
    goal_tokens_.erase(goal_handle.get());
    return;
  }

  if (rclcpp::ok()) {
    result->success = true;
    try {
      goal_handle->succeed(result);
    } catch (const rclcpp::exceptions::RCLError & e) {
      RCLCPP_WARN(get_logger(), "Failed to mark goal succeeded (RCLError): %s", e.what());
    }
  }

  std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
  goal_tokens_.erase(goal_handle.get());
}

void LedsPlayActionServer::publishOff(const std::array<uint8_t, 2> & leds)
{
  if (has_led(leds, LedIndexes::HEAD)) {
    nao_lola_command_msgs::msg::HeadLeds msg;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
      msg.intensities[i] = 0.0F;
    }
    head_pub_->publish(msg);
  }

  if (has_led(leds, LedIndexes::REYE)) {
    nao_lola_command_msgs::msg::RightEyeLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
      r.colors[i] = color_off_;
    }
    right_eye_pub_->publish(r);
  }

  if (has_led(leds, LedIndexes::LEYE)) {
    nao_lola_command_msgs::msg::LeftEyeLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
      l.colors[i] = color_off_;
    }
    left_eye_pub_->publish(l);
  }

  if (has_led(leds, LedIndexes::REAR)) {
    nao_lola_command_msgs::msg::RightEarLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
      r.intensities[i] = 0.0F;
    }
    right_ear_pub_->publish(r);
  }

  if (has_led(leds, LedIndexes::LEAR)) {
    nao_lola_command_msgs::msg::LeftEarLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
      l.intensities[i] = 0.0F;
    }
    left_ear_pub_->publish(l);
  }

  if (has_led(leds, LedIndexes::CHEST)) {
    nao_lola_command_msgs::msg::ChestLed msg;
    msg.color = color_off_;
    chest_pub_->publish(msg);
  }

  if (has_led(leds, LedIndexes::RFOOT)) {
    nao_lola_command_msgs::msg::RightFootLed msg;
    msg.color = color_off_;
    right_foot_pub_->publish(msg);
  }

  if (has_led(leds, LedIndexes::LFOOT)) {
    nao_lola_command_msgs::msg::LeftFootLed msg;
    msg.color = color_off_;
    left_foot_pub_->publish(msg);
  }
}

bool LedsPlayActionServer::steadyMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors_in,
  const std::array<float, 12> & intensities,
  float duration_s)
{
  const auto eye_colors = normalize_eye_colors(colors_in);

  // tokens snapshot for this goal
  std::array<std::uint64_t, kNumEffectors> tokens{};
  tokens.fill(0);
  {
    std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
    auto it = goal_tokens_.find(goal_handle.get());
    if (it != goal_tokens_.end()) {
      tokens = it->second;
    }
  }

  auto owned = [&](uint8_t eff) -> bool {
    if (!is_valid_eff(eff) || !has_led(leds, eff)) {
      return false;
    }
    const auto tok = tokens[eff];
    if (tok == 0) {
      return false;
    }
    return eff_gen_[eff].load(std::memory_order_acquire) == tok;
  };

  auto publish_off_owned = [&]() {
    // Only turn off effectors that are still owned by this goal
    if (owned(LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
        msg.intensities[i] = 0.0F;
      }
      head_pub_->publish(msg);
    }
    if (owned(LedIndexes::REYE)) {
      nao_lola_command_msgs::msg::RightEyeLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
        r.colors[i] = color_off_;
      }
      right_eye_pub_->publish(r);
    }
    if (owned(LedIndexes::LEYE)) {
      nao_lola_command_msgs::msg::LeftEyeLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
        l.colors[i] = color_off_;
      }
      left_eye_pub_->publish(l);
    }
    if (owned(LedIndexes::REAR)) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
        r.intensities[i] = 0.0F;
      }
      right_ear_pub_->publish(r);
    }
    if (owned(LedIndexes::LEAR)) {
      nao_lola_command_msgs::msg::LeftEarLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
        l.intensities[i] = 0.0F;
      }
      left_ear_pub_->publish(l);
    }
    if (owned(LedIndexes::CHEST)) {
      nao_lola_command_msgs::msg::ChestLed msg;
      msg.color = color_off_;
      chest_pub_->publish(msg);
    }
    if (owned(LedIndexes::RFOOT)) {
      nao_lola_command_msgs::msg::RightFootLed msg;
      msg.color = color_off_;
      right_foot_pub_->publish(msg);
    }
    if (owned(LedIndexes::LFOOT)) {
      nao_lola_command_msgs::msg::LeftFootLed msg;
      msg.color = color_off_;
      left_foot_pub_->publish(msg);
    }
  };

  // Apply steady state ONCE for each effector still owned
  if (owned(LedIndexes::HEAD)) {
    nao_lola_command_msgs::msg::HeadLeds msg;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
      msg.intensities[i] = intensities[i];
    }
    head_pub_->publish(msg);
  }

  if (owned(LedIndexes::REYE)) {
    nao_lola_command_msgs::msg::RightEyeLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
      r.colors[i] = eye_colors[i];
    }
    right_eye_pub_->publish(r);
  }

  if (owned(LedIndexes::LEYE)) {
    nao_lola_command_msgs::msg::LeftEyeLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
      l.colors[i] = eye_colors[i];
    }
    left_eye_pub_->publish(l);
  }

  if (owned(LedIndexes::REAR)) {
    nao_lola_command_msgs::msg::RightEarLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
      r.intensities[i] = intensities[i];
    }
    right_ear_pub_->publish(r);
  }

  if (owned(LedIndexes::LEAR)) {
    nao_lola_command_msgs::msg::LeftEarLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
      l.intensities[i] = intensities[i];
    }
    left_ear_pub_->publish(l);
  }

  if (owned(LedIndexes::CHEST)) {
    nao_lola_command_msgs::msg::ChestLed msg;
    msg.color = eye_colors[0];
    chest_pub_->publish(msg);
  }

  if (owned(LedIndexes::RFOOT)) {
    nao_lola_command_msgs::msg::RightFootLed msg;
    msg.color = eye_colors[0];
    right_foot_pub_->publish(msg);
  }

  if (owned(LedIndexes::LFOOT)) {
    nao_lola_command_msgs::msg::LeftFootLed msg;
    msg.color = eye_colors[0];
    left_foot_pub_->publish(msg);
  }

  auto any_owned_now = [&]() -> bool {
    for (uint8_t eff = 0; eff < kNumEffectors; ++eff) {
      if (owned(eff)) {
        return true;
      }
    }
    return false;
  };

  if (duration_s < 0.0F) {
    rclcpp::Rate rate(50.0);
    while (!shouldStop(goal_handle)) {
      if (!any_owned_now()) {
        // fully preempted
        return true;
      }
      rate.sleep();
    }
    publish_off_owned();
    return true;
  }

  if (duration_s == 0.0F) {
    // keep it one-shot and do not auto-off (matches your prior intent)
    // but to keep semantics consistent with your newer server, we do off only if still owned
    publish_off_owned();
    return false;
  }

  const rclcpp::Time start = now();
  rclcpp::Rate rate(50.0);
  while (!shouldStop(goal_handle)) {
    if (!any_owned_now()) {
      return true;  // fully preempted
    }
    if ((now() - start).seconds() >= duration_s) {
      break;
    }
    rate.sleep();
  }

  if (shouldStop(goal_handle)) {
    publish_off_owned();
    return true;
  }

  publish_off_owned();
  return false;
}

bool LedsPlayActionServer::blinkingMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  float frequency_hz,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors_in,
  float duration_s)
{
  if (frequency_hz <= 0.0F) {
    // nothing to do; treat as early stop
    return true;
  }

  const auto eye_colors = normalize_eye_colors(colors_in);

  // tokens snapshot for this goal
  std::array<std::uint64_t, kNumEffectors> tokens{};
  tokens.fill(0);
  {
    std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
    auto it = goal_tokens_.find(goal_handle.get());
    if (it != goal_tokens_.end()) {
      tokens = it->second;
    }
  }

  auto owned = [&](uint8_t eff) -> bool {
    if (!is_valid_eff(eff) || !has_led(leds, eff)) {
      return false;
    }
    const auto tok = tokens[eff];
    if (tok == 0) {
      return false;
    }
    return eff_gen_[eff].load(std::memory_order_acquire) == tok;
  };

  auto any_owned_now = [&]() -> bool {
    for (uint8_t eff = 0; eff < kNumEffectors; ++eff) {
      if (owned(eff)) {
        return true;
      }
    }
    return false;
  };

  // publish off only for effectors still owned by this goal
  auto publish_off_owned = [&]() {
    if (owned(LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
        msg.intensities[i] = 0.0F;
      }
      head_pub_->publish(msg);
    }
    if (owned(LedIndexes::REYE)) {
      nao_lola_command_msgs::msg::RightEyeLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
        r.colors[i] = color_off_;
      }
      right_eye_pub_->publish(r);
    }
    if (owned(LedIndexes::LEYE)) {
      nao_lola_command_msgs::msg::LeftEyeLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
        l.colors[i] = color_off_;
      }
      left_eye_pub_->publish(l);
    }
    if (owned(LedIndexes::REAR)) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
        r.intensities[i] = 0.0F;
      }
      right_ear_pub_->publish(r);
    }
    if (owned(LedIndexes::LEAR)) {
      nao_lola_command_msgs::msg::LeftEarLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
        l.intensities[i] = 0.0F;
      }
      left_ear_pub_->publish(l);
    }
    if (owned(LedIndexes::CHEST)) {
      nao_lola_command_msgs::msg::ChestLed msg;
      msg.color = color_off_;
      chest_pub_->publish(msg);
    }
    if (owned(LedIndexes::RFOOT)) {
      nao_lola_command_msgs::msg::RightFootLed msg;
      msg.color = color_off_;
      right_foot_pub_->publish(msg);
    }
    if (owned(LedIndexes::LFOOT)) {
      nao_lola_command_msgs::msg::LeftFootLed msg;
      msg.color = color_off_;
      left_foot_pub_->publish(msg);
    }
  };

  const rclcpp::Time start = now();
  rclcpp::Rate loop_rate(frequency_hz);
  bool on = false;

  while (!shouldStop(goal_handle)) {
    if (!any_owned_now()) {
      // fully preempted
      return true;
    }

    if (duration_s >= 0.0F && (now() - start).seconds() >= duration_s) {
      break;
    }

    on = !on;

    if (owned(LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
        msg.intensities[i] = on ? 1.0F : 0.0F;
      }
      head_pub_->publish(msg);
    }

    if (owned(LedIndexes::REYE)) {
      nao_lola_command_msgs::msg::RightEyeLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
        r.colors[i] = on ? eye_colors[i] : color_off_;
      }
      right_eye_pub_->publish(r);
    }

    if (owned(LedIndexes::LEYE)) {
      nao_lola_command_msgs::msg::LeftEyeLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
        l.colors[i] = on ? eye_colors[i] : color_off_;
      }
      left_eye_pub_->publish(l);
    }

    if (owned(LedIndexes::REAR)) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
        r.intensities[i] = on ? 1.0F : 0.0F;
      }
      right_ear_pub_->publish(r);
    }

    if (owned(LedIndexes::LEAR)) {
      nao_lola_command_msgs::msg::LeftEarLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
        l.intensities[i] = on ? 1.0F : 0.0F;
      }
      left_ear_pub_->publish(l);
    }

    if (owned(LedIndexes::CHEST)) {
      nao_lola_command_msgs::msg::ChestLed msg;
      msg.color = on ? eye_colors[0] : color_off_;
      chest_pub_->publish(msg);
    }

    if (owned(LedIndexes::RFOOT)) {
      nao_lola_command_msgs::msg::RightFootLed msg;
      msg.color = on ? eye_colors[0] : color_off_;
      right_foot_pub_->publish(msg);
    }

    if (owned(LedIndexes::LFOOT)) {
      nao_lola_command_msgs::msg::LeftFootLed msg;
      msg.color = on ? eye_colors[0] : color_off_;
      left_foot_pub_->publish(msg);
    }

    loop_rate.sleep();
  }

  // turn off only what we still own
  publish_off_owned();
  return shouldStop(goal_handle);
}

bool LedsPlayActionServer::loopMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  float frequency_hz,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors_in,
  float duration_s)
{
  if (frequency_hz <= 0.0F) {
    return true;
  }

  const auto eye_colors = normalize_eye_colors(colors_in);

  // tokens snapshot for this goal
  std::array<std::uint64_t, kNumEffectors> tokens{};
  tokens.fill(0);
  {
    std::lock_guard<std::mutex> lk(goal_tokens_mtx_);
    auto it = goal_tokens_.find(goal_handle.get());
    if (it != goal_tokens_.end()) {
      tokens = it->second;
    }
  }

  auto owned = [&](uint8_t eff) -> bool {
    if (!is_valid_eff(eff) || !has_led(leds, eff)) {
      return false;
    }
    const auto tok = tokens[eff];
    if (tok == 0) {
      return false;
    }
    return eff_gen_[eff].load(std::memory_order_acquire) == tok;
  };

  auto any_owned_now = [&]() -> bool {
    for (uint8_t eff = 0; eff < kNumEffectors; ++eff) {
      if (owned(eff)) {
        return true;
      }
    }
    return false;
  };

  auto publish_off_owned = [&]() {
    if (owned(LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
        msg.intensities[i] = 0.0F;
      }
      head_pub_->publish(msg);
    }
    if (owned(LedIndexes::REYE)) {
      nao_lola_command_msgs::msg::RightEyeLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
        r.colors[i] = color_off_;
      }
      right_eye_pub_->publish(r);
    }
    if (owned(LedIndexes::LEYE)) {
      nao_lola_command_msgs::msg::LeftEyeLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
        l.colors[i] = color_off_;
      }
      left_eye_pub_->publish(l);
    }
    if (owned(LedIndexes::REAR)) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
        r.intensities[i] = 0.0F;
      }
      right_ear_pub_->publish(r);
    }
    if (owned(LedIndexes::LEAR)) {
      nao_lola_command_msgs::msg::LeftEarLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
        l.intensities[i] = 0.0F;
      }
      left_ear_pub_->publish(l);
    }
    if (owned(LedIndexes::CHEST)) {
      nao_lola_command_msgs::msg::ChestLed msg;
      msg.color = color_off_;
      chest_pub_->publish(msg);
    }
    if (owned(LedIndexes::RFOOT)) {
      nao_lola_command_msgs::msg::RightFootLed msg;
      msg.color = color_off_;
      right_foot_pub_->publish(msg);
    }
    if (owned(LedIndexes::LFOOT)) {
      nao_lola_command_msgs::msg::LeftFootLed msg;
      msg.color = color_off_;
      left_foot_pub_->publish(msg);
    }
  };

  const rclcpp::Time start = now();
  rclcpp::Rate loop_rate(frequency_hz);

  uint8_t head_c = 0;
  uint8_t ear_c = 0;
  int eye_step = 0;

  const int eye_n  = nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS;
  const int ear_n  = nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS;
  const int head_n = nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS;

  while (!shouldStop(goal_handle)) {
    if (!any_owned_now()) {
      return true;
    }

    if (duration_s >= 0.0F && (now() - start).seconds() >= duration_s) {
      break;
    }

    if (owned(LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (int i = 0; i < head_n; ++i) {
        msg.intensities[i] = 1.0F;
      }
      head_c = static_cast<uint8_t>((head_c + 1U) % head_n);
      msg.intensities[head_c] = 0.0F;
      head_pub_->publish(msg);
    }

    // Eyes: compute once per tick, but publish per-eye only if owned
    const bool do_reye = owned(LedIndexes::REYE);
    const bool do_leye = owned(LedIndexes::LEYE);
    if (do_reye || do_leye) {
      eye_step = (eye_step + 1) % eye_n;

      const int cw = eye_step;
      const int cw_succ = (cw + 1) % eye_n;
      const int ccw = (cw != 0) ? (eye_n - cw) : 0;
      const int ccw_succ = (ccw != 0) ? (ccw - 1) : (eye_n - 1);

      if (do_reye) {
        nao_lola_command_msgs::msg::RightEyeLeds r;
        for (int i = 0; i < eye_n; ++i) {
          r.colors[i] = eye_colors[i];
        }
        r.colors[cw] = color_off_;
        r.colors[cw_succ] = color_off_;
        right_eye_pub_->publish(r);
      }

      if (do_leye) {
        nao_lola_command_msgs::msg::LeftEyeLeds l;
        for (int i = 0; i < eye_n; ++i) {
          l.colors[i] = eye_colors[i];
        }
        l.colors[ccw] = color_off_;
        l.colors[ccw_succ] = color_off_;
        left_eye_pub_->publish(l);
      }
    }

    if (owned(LedIndexes::REAR)) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      for (int i = 0; i < ear_n; ++i) {
        r.intensities[i] = 1.0F;
      }
      ear_c = static_cast<uint8_t>((ear_c + 1U) % ear_n);
      r.intensities[ear_c] = 0.0F;
      right_ear_pub_->publish(r);
    }

    if (owned(LedIndexes::LEAR)) {
      nao_lola_command_msgs::msg::LeftEarLeds l;
      for (int i = 0; i < ear_n; ++i) {
        l.intensities[i] = 1.0F;
      }
      ear_c = static_cast<uint8_t>((ear_c + 1U) % ear_n);
      l.intensities[ear_c] = 0.0F;
      left_ear_pub_->publish(l);
    }

    if (owned(LedIndexes::CHEST)) {
      nao_lola_command_msgs::msg::ChestLed msg;
      msg.color = eye_colors[0];
      chest_pub_->publish(msg);
    }

    if (owned(LedIndexes::RFOOT)) {
      nao_lola_command_msgs::msg::RightFootLed msg;
      msg.color = eye_colors[0];
      right_foot_pub_->publish(msg);
    }

    if (owned(LedIndexes::LFOOT)) {
      nao_lola_command_msgs::msg::LeftFootLed msg;
      msg.color = eye_colors[0];
      left_foot_pub_->publish(msg);
    }

    loop_rate.sleep();
  }

  publish_off_owned();
  return shouldStop(goal_handle);
}

}  // namespace nao_led_action_server

RCLCPP_COMPONENTS_REGISTER_NODE(nao_led_action_server::LedsPlayActionServer)
